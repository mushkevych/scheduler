__author__ = 'Bohdan Mushkevych'

from datetime import datetime
from logging import ERROR, WARNING, INFO

from synergy.conf import context
from synergy.db.error import DuplicateKeyError
from synergy.db.model import unit_of_work
from synergy.db.model.unit_of_work import UnitOfWork
from synergy.db.model.synergy_mq_transmission import SynergyMqTransmission
from synergy.db.model.freerun_process_entry import FreerunProcessEntry, MAX_NUMBER_OF_LOG_ENTRIES
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.dao.freerun_process_dao import FreerunProcessDao
from synergy.system import time_helper
from synergy.system.time_qualifier import QUALIFIER_REAL_TIME
from synergy.system.decorator import with_reconnect
from synergy.scheduler.scheduler_constants import STATE_MACHINE_FREERUN, TYPE_FREERUN
from synergy.mq.flopsy import PublishersPool


class StateMachineFreerun(object):
    """ State Machine to handle freerun jobs/triggers """

    def __init__(self, logger, name=STATE_MACHINE_FREERUN):
        self.name = name
        self.logger = logger
        self.publishers = PublishersPool(self.logger)
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.sfe_dao = FreerunProcessDao(self.logger)

    def __del__(self):
        try:
            self.logger.info('Closing Flopsy Publishers Pool...')
            self.publishers.close()
        except Exception as e:
            self.logger.error('Exception caught while closing Flopsy Publishers Pool: %s' % str(e))

    @with_reconnect
    def _log_message(self, level, freerun_entry, msg):
        """ method performs logging into log file and the freerun_entry """
        self.logger.log(level, msg)

        assert isinstance(freerun_entry, FreerunProcessEntry)
        log = freerun_entry.log
        if len(log) > MAX_NUMBER_OF_LOG_ENTRIES:
            del log[-1]
        log.insert(0, msg)
        self.sfe_dao.update(freerun_entry)

    @with_reconnect
    def _insert_uow(self, freerun_entry):
        """ creates unit_of_work and inserts it into the DB
            :raise DuplicateKeyError: if unit_of_work with given parameters already exists """
        current_timeperiod = time_helper.actual_timeperiod(QUALIFIER_REAL_TIME)

        uow = UnitOfWork()
        uow.process_name = freerun_entry.schedulable_name
        uow.timeperiod = current_timeperiod
        uow.start_id = 0
        uow.end_id = 0
        uow.start_timeperiod = current_timeperiod
        uow.end_timeperiod = current_timeperiod
        uow.created_at = datetime.utcnow()
        uow.source = context.process_context[freerun_entry.process_name].source
        uow.sink = context.process_context[freerun_entry.process_name].sink
        uow.state = unit_of_work.STATE_REQUESTED
        uow.unit_of_work_type = TYPE_FREERUN
        uow.number_of_retries = 0
        uow.arguments = freerun_entry.arguments
        uow.db_id = self.uow_dao.insert(uow)

        msg = 'Created: UOW %s for %s in timeperiod %s.' \
              % (uow.db_id, freerun_entry.schedulable_name, current_timeperiod)
        self._log_message(INFO, freerun_entry, msg)
        return uow

    def _publish_uow(self, freerun_entry, uow):
        mq_request = SynergyMqTransmission(process_name=freerun_entry.process_name,
                                           entry_name=freerun_entry.entry_name,
                                           unit_of_work_id=uow.db_id)

        publisher = self.publishers.get(freerun_entry.process_name)
        publisher.publish(mq_request.document)
        publisher.release()

        msg = 'Published: UOW %s for %s.' % (uow.db_id, freerun_entry.schedulable_name)
        self._log_message(INFO, freerun_entry, msg)

    def insert_and_publish_uow(self, freerun_entry):
        try:
            uow = self._insert_uow(freerun_entry)
        except DuplicateKeyError as e:
            msg = 'Duplication of unit_of_work found for %s. Error msg: %r' % (freerun_entry.schedulable_name, e)
            self._log_message(WARNING, freerun_entry, msg)
            uow = self.uow_dao.recover_from_duplicatekeyerror(e)

        if uow is not None:
            # publish the created/caught up unit_of_work
            self._publish_uow(freerun_entry, uow)
            freerun_entry.related_unit_of_work = uow.db_id
            self.sfe_dao.update(freerun_entry)
        else:
            msg = 'SYSTEM IS LIKELY IN UNSTABLE STATE! Unable to locate unit_of_work for %s' \
                  % freerun_entry.schedulable_name
            self._log_message(WARNING, freerun_entry, msg)

    def manage_schedulable(self, freerun_entry):
        """ method main duty - is to _avoid_ publishing another unit_of_work, if previous was not yet processed
        In case the Scheduler sees that the unit_of_work is pending it will fire another WorkerMqRequest """

        assert isinstance(freerun_entry, FreerunProcessEntry)
        if freerun_entry.related_unit_of_work is None:
            uow = None
        else:
            uow = self.uow_dao.get_one(freerun_entry.related_unit_of_work)

        try:
            if uow is None:
                self._process_state_embryo(freerun_entry)

            elif uow.is_requested or uow.is_in_progress:
                self._process_state_in_progress(freerun_entry, uow)

            elif uow.is_finished or uow.is_invalid:
                self._process_terminal_state(freerun_entry, uow)

            else:
                msg = 'Unknown state %s of the unit_of_work %s' % (uow.state, uow.db_id)
                self._log_message(ERROR, freerun_entry, msg)

        except LookupError as e:
            msg = 'Lookup issue for schedulable: %r in timeperiod %s, because of: %r' \
                  % (freerun_entry.db_id, uow.timeperiod, e)
            self._log_message(WARNING, freerun_entry, msg)

    def _process_state_embryo(self, freerun_entry):
        """ method creates unit_of_work and associates it with the FreerunProcessEntry """
        self.insert_and_publish_uow(freerun_entry)

    def _process_state_in_progress(self, freerun_entry, uow):
        """ method that takes care of processing unit_of_work records in STATE_REQUESTED or STATE_IN_PROGRESS states"""
        self._publish_uow(freerun_entry, uow)

    def _process_terminal_state(self, freerun_entry, uow):
        """ method that takes care of processing unit_of_work records in
        STATE_PROCESSED, STATE_INVALID, STATE_CANCELED states"""
        msg = 'unit_of_work for %s found in %s state.' % (freerun_entry.schedulable_name, uow.state)
        self._log_message(INFO, freerun_entry, msg)
        self.insert_and_publish_uow(freerun_entry)
