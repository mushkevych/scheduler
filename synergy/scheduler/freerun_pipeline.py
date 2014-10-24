__author__ = 'Bohdan Mushkevych'

from datetime import datetime
from logging import ERROR, WARNING, INFO

from synergy.conf.process_context import ProcessContext
from synergy.db.error import DuplicateKeyError
from synergy.db.model import unit_of_work
from synergy.db.model.unit_of_work import UnitOfWork
from synergy.db.model.worker_mq_request import WorkerMqRequest
from synergy.db.model.scheduler_freerun_entry import SchedulerFreerunEntry, MAX_NUMBER_OF_LOG_ENTRIES
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.dao.scheduler_freerun_entry_dao import SchedulerFreerunEntryDao
from synergy.system import time_helper
from synergy.system.time_qualifier import QUALIFIER_REAL_TIME
from synergy.system.decorator import with_reconnect
from synergy.scheduler.scheduler_constants import PIPELINE_FREERUN, TYPE_FREERUN
from synergy.mq.flopsy import PublishersPool


class FreerunPipeline(object):
    """ Pipeline to handle freerun jobs/triggers """

    def __init__(self, logger, name=PIPELINE_FREERUN):
        self.name = name
        self.logger = logger
        self.publishers = PublishersPool(self.logger)
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.sfe_dao = SchedulerFreerunEntryDao(self.logger)

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

        assert isinstance(freerun_entry, SchedulerFreerunEntry)
        log = freerun_entry.log
        if len(log) > MAX_NUMBER_OF_LOG_ENTRIES:
            del log[-1]
        log.insert(0, msg)
        self.sfe_dao.update(freerun_entry)

    @with_reconnect
    def insert_uow(self, freerun_entry):
        """ creates unit_of_work and inserts it into the DB
            :raise DuplicateKeyError: if unit_of_work with given parameters already exists """
        schedulable_name = '%s::%s' % (freerun_entry.process_name, freerun_entry.entry_name)
        current_timeperiod = time_helper.actual_timeperiod(QUALIFIER_REAL_TIME)

        uow = UnitOfWork()
        uow.process_name = schedulable_name
        uow.timeperiod = current_timeperiod
        uow.start_id = 0
        uow.end_id = 0
        uow.start_timeperiod = current_timeperiod
        uow.end_timeperiod = current_timeperiod
        uow.created_at = datetime.utcnow()
        uow.source = ProcessContext.get_source(freerun_entry.process_name)
        uow.sink = ProcessContext.get_source(freerun_entry.process_name)
        uow.state = unit_of_work.STATE_REQUESTED
        uow.unit_of_work_type = TYPE_FREERUN
        uow.number_of_retries = 0
        uow.arguments = freerun_entry.arguments
        uow.document['_id'] = self.uow_dao.insert(uow)

        msg = 'Created: UOW %s for %s in timeperiod %s.' % (uow.db_id, schedulable_name, current_timeperiod)
        self._log_message(INFO, freerun_entry, msg)
        return uow

    def publish_uow(self, freerun_entry, uow):
        schedulable_name = '%s::%s' % (freerun_entry.process_name, freerun_entry.entry_name)
        mq_request = WorkerMqRequest()
        mq_request.process_name = freerun_entry.process_name
        mq_request.entry_name = freerun_entry.entry_name
        mq_request.unit_of_work_id = uow.db_id

        publisher = self.publishers.get(freerun_entry.process_name)
        publisher.publish(mq_request.document)
        publisher.release()

        msg = 'Published: UOW %s for %s.' % (uow.db_id, schedulable_name)
        self._log_message(INFO, freerun_entry, msg)

    def manage_pipeline_for_schedulable(self, freerun_entry):
        """ method main duty - is to _avoid_ publishing another unit_of_work, if previous was not yet processed
        In case the Scheduler sees that the unit_of_work is pending it will fire another WorkerMqRequest """

        assert isinstance(freerun_entry, SchedulerFreerunEntry)
        if freerun_entry.related_unit_of_work is None:
            uow_record = None
        else:
            uow_record = self.uow_dao.get_one(freerun_entry.related_unit_of_work)

        try:
            if uow_record is None:
                self._process_state_embryo(freerun_entry)

            elif uow_record.state in [unit_of_work.STATE_REQUESTED, unit_of_work.STATE_IN_PROGRESS]:
                self._process_state_in_progress(freerun_entry, uow_record)

            elif uow_record.state in [unit_of_work.STATE_PROCESSED,
                                      unit_of_work.STATE_INVALID,
                                      unit_of_work.STATE_CANCELED]:
                self._process_terminal_state(freerun_entry, uow_record)

            else:
                msg = 'Unknown state %s of the unit_of_work %s' % (uow_record.state, uow_record.db_id)
                self._log_message(ERROR, freerun_entry, msg)

        except LookupError as e:
            msg = 'Lookup issue for schedulable: %r in timeperiod %s, because of: %r' \
                  % (freerun_entry.db_id, uow_record.timeperiod, e)
            self._log_message(WARNING, freerun_entry, msg)

    def _process_state_embryo(self, freerun_entry):
        """ method creates unit_of_work and associates it with the SchedulerFreerunEntry """
        try:
            uow = self.insert_uow(freerun_entry)
            self.publish_uow(freerun_entry, uow)
            freerun_entry.related_unit_of_work = uow.db_id
            self.sfe_dao.update(freerun_entry)
        except DuplicateKeyError as e:
            msg = 'Duplication of unit_of_work found for %s::%s. Ignoring this request. Error msg: %r' \
                  % (freerun_entry.process_name, freerun_entry.entry_name, e)
            self._log_message(WARNING, freerun_entry, msg)

    def _process_state_in_progress(self, freerun_entry, uow_record):
        """ method that takes care of processing unit_of_work records in STATE_REQUESTED or STATE_IN_PROGRESS states"""
        self.publish_uow(freerun_entry, uow_record)

    def _process_terminal_state(self, freerun_entry, uow_record):
        """ method that takes care of processing unit_of_work records in
        STATE_PROCESSED, STATE_INVALID, STATE_CANCELED states"""
        msg = 'unit_of_work for %s::%s found in %s state.' \
              % (freerun_entry.process_name, freerun_entry.entry_name, uow_record.state)
        self._log_message(INFO, freerun_entry, msg)
        try:
            uow = self.insert_uow(freerun_entry)
            self.publish_uow(freerun_entry, uow)
            freerun_entry.related_unit_of_work = uow.db_id
            self.sfe_dao.update(freerun_entry)
        except DuplicateKeyError as e:
            msg = 'Duplication of unit_of_work found for %s::%s. Ignoring this request. Error msg: %r' \
                  % (freerun_entry.process_name, freerun_entry.entry_name, e)
            self._log_message(WARNING, freerun_entry, msg)
