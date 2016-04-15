__author__ = 'Bohdan Mushkevych'

from datetime import datetime
from logging import ERROR, WARNING, INFO

from synergy.conf import context
from synergy.db.error import DuplicateKeyError
from synergy.db.model import unit_of_work
from synergy.db.model.unit_of_work import UnitOfWork
from synergy.db.model.freerun_process_entry import FreerunProcessEntry, MAX_NUMBER_OF_EVENTS
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.dao.freerun_process_dao import FreerunProcessDao
from synergy.system import time_helper
from synergy.system.time_qualifier import QUALIFIER_REAL_TIME
from synergy.system.decorator import with_reconnect
from synergy.system.mq_transmitter import MqTransmitter
from synergy.scheduler.scheduler_constants import STATE_MACHINE_FREERUN


class StateMachineFreerun(object):
    """ State Machine to handle freerun jobs/triggers """

    def __init__(self, logger, name=STATE_MACHINE_FREERUN):
        self.name = name
        self.logger = logger
        self.mq_transmitter = MqTransmitter(self.logger)
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.sfe_dao = FreerunProcessDao(self.logger)

    @with_reconnect
    def _log_message(self, level, freerun_entry, msg):
        """ method performs logging into log file and the freerun_entry """
        self.logger.log(level, msg)

        assert isinstance(freerun_entry, FreerunProcessEntry)
        event_log = freerun_entry.event_log
        if len(event_log) > MAX_NUMBER_OF_EVENTS:
            del event_log[-1]
        event_log.insert(0, msg)
        self.sfe_dao.update(freerun_entry)

    @with_reconnect
    def _insert_uow(self, freerun_entry):
        """ creates unit_of_work and inserts it into the DB
            :raise DuplicateKeyError: if unit_of_work with given parameters already exists """
        current_timeperiod = time_helper.actual_timeperiod(QUALIFIER_REAL_TIME)
        process_entry = context.process_context[freerun_entry.process_name]

        uow = UnitOfWork()
        uow.process_name = freerun_entry.schedulable_name
        uow.timeperiod = current_timeperiod
        uow.start_id = 0
        uow.end_id = 0
        uow.start_timeperiod = current_timeperiod
        uow.end_timeperiod = current_timeperiod
        uow.created_at = datetime.utcnow()
        uow.submitted_at = datetime.utcnow()
        uow.source = process_entry.source if hasattr(process_entry, 'source') else None
        uow.sink = process_entry.sink if hasattr(process_entry, 'sink') else None
        uow.state = unit_of_work.STATE_REQUESTED
        uow.unit_of_work_type = unit_of_work.TYPE_FREERUN
        uow.number_of_retries = 0
        uow.arguments = freerun_entry.arguments
        uow.db_id = self.uow_dao.insert(uow)

        msg = 'Created: UOW {0} for {1}@{2}.' \
              .format(uow.db_id, freerun_entry.schedulable_name, current_timeperiod)
        self._log_message(INFO, freerun_entry, msg)
        return uow

    def _publish_uow(self, freerun_entry, uow):
        self.mq_transmitter.publish_freerun_uow(freerun_entry, uow)
        msg = 'Published: UOW {0} for {1}.'.format(uow.db_id, freerun_entry.schedulable_name)
        self._log_message(INFO, freerun_entry, msg)

    def insert_and_publish_uow(self, freerun_entry):
        try:
            uow = self._insert_uow(freerun_entry)
        except DuplicateKeyError as e:
            msg = 'Duplication of UOW found for {0}. Error msg: {1}'.format(freerun_entry.schedulable_name, e)
            self._log_message(WARNING, freerun_entry, msg)
            uow = self.uow_dao.recover_from_duplicatekeyerror(e)

        if uow is not None:
            # publish the created/caught up unit_of_work
            self._publish_uow(freerun_entry, uow)
            freerun_entry.related_unit_of_work = uow.db_id
            self.sfe_dao.update(freerun_entry)
        else:
            msg = 'PERSISTENT TIER ERROR! Unable to locate UOW for {0}' \
                  .format(freerun_entry.schedulable_name)
            self._log_message(WARNING, freerun_entry, msg)

    def _process_state_embryo(self, freerun_entry):
        """ method creates unit_of_work and associates it with the FreerunProcessEntry """
        self.insert_and_publish_uow(freerun_entry)

    def _process_state_in_progress(self, freerun_entry, uow):
        """ method that takes care of processing unit_of_work records in STATE_REQUESTED or STATE_IN_PROGRESS states"""
        self._publish_uow(freerun_entry, uow)

    def _process_terminal_state(self, freerun_entry, uow):
        """ method that takes care of processing unit_of_work records in
            STATE_PROCESSED, STATE_NOOP, STATE_INVALID, STATE_CANCELED states"""
        msg = 'UOW for {0} found in state {1}.'.format(freerun_entry.schedulable_name, uow.state)
        self._log_message(INFO, freerun_entry, msg)
        self.insert_and_publish_uow(freerun_entry)

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
                msg = 'Unknown state {0} of the UOW {1}'.format(uow.state, uow.db_id)
                self._log_message(ERROR, freerun_entry, msg)

        except LookupError as e:
            msg = 'Lookup issue for schedulable: {0} in timeperiod {1}, because of: {2}' \
                  .format(freerun_entry.db_id, uow.timeperiod, e)
            self._log_message(WARNING, freerun_entry, msg)

    def cancel_uow(self, freerun_entry):
        uow_id = freerun_entry.related_unit_of_work
        if uow_id is None:
            msg = 'cancel_uow: no related UOW for {0}'.format(freerun_entry.schedulable_name)
        else:
            uow = self.uow_dao.get_one(uow_id)
            uow.state = unit_of_work.STATE_CANCELED
            self.uow_dao.update(uow)
            msg = 'cancel_uow: canceled UOW {0} for {1}'.format(uow_id, freerun_entry.schedulable_name)
        self._log_message(INFO, freerun_entry, msg)
