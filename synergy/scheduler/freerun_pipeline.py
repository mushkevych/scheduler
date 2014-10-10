from synergy.system.time_qualifier import QUALIFIER_REAL_TIME

__author__ = 'Bohdan Mushkevych'

from datetime import datetime
from logging import ERROR, WARNING, INFO

from synergy.db.error import DuplicateKeyError
from synergy.db.model import unit_of_work
from synergy.db.model.unit_of_work import UnitOfWork
from synergy.db.model.worker_mq_request import WorkerMqRequest
from synergy.db.model.scheduler_freerun_entry import SchedulerFreerunEntry, MAX_NUMBER_OF_LOG_ENTRIES
from synergy.scheduler.scheduler_constants import PIPELINE_FREERUN
from synergy.system import time_helper
from synergy.conf.process_context import ProcessContext

from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.dao.scheduler_freerun_entry_dao import SchedulerFreerunEntryDao
from synergy.mq.flopsy import PublishersPool
from synergy.system.decorator import with_reconnect


class FreerunPipeline(object):
    """ Pipeline to handle freerun jobs/triggers """

    def __init__(self, logger, timetable, name=PIPELINE_FREERUN):
        self.name = name
        self.logger = logger
        self.publishers = PublishersPool(self.logger)
        self.timetable = timetable
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
    def recover_from_duplicatekeyerror(self, e):
        """ try to recover from DuplicateKeyError """
        start_id = None
        end_id = None
        process_name = None
        timeperiod = None

        if hasattr(e, 'start_id'):
            start_id = e.start_id
        if hasattr(e, 'end_id'):
            end_id = e.end_id
        if hasattr(e, 'process_name'):
            process_name = e.process_name
        if hasattr(e, 'timeperiod'):
            timeperiod = e.timeperiod

        if process_name is not None \
                and timeperiod is not None \
                and start_id is not None \
                and end_id is not None:
            try:
                return self.uow_dao.get_by_params(process_name, timeperiod, start_id, end_id)
            except LookupError as e:
                self.logger.error('Unable to recover from DB error due to %s' % e.message, exc_info=True)
        else:
            msg = 'Unable to locate unit_of_work due to incomplete primary key ' \
                  '(process_name=%s, timeperiod=%s, start_id=%s, end_id=%s)' \
                  % (process_name, timeperiod, start_id, end_id)
            self.logger.error(msg)

    def manage_pipeline_for_schedulable(self, freerun_entry):
        """ method main duty - is to _avoid_ publishing another unit_of_work, if previous was not yet processed
        In case the Scheduler sees that the unit_of_work is pending it will fire another WorkerMqRequest """

        assert isinstance(freerun_entry, SchedulerFreerunEntry)
        uow_record = freerun_entry.related_unit_of_work

        try:
            if uow_record is None:
                self._process_state_embryo(freerun_entry)

            elif uow_record.state == unit_of_work.STATE_REQUESTED:
                self._process_state_requested(freerun_entry, uow_record)

            elif uow_record.state == unit_of_work.STATE_IN_PROGRESS:
                self._process_state_in_progress(freerun_entry, uow_record)

            elif uow_record.state == unit_of_work.STATE_PROCESSED:
                self._process_state_processed(freerun_entry, uow_record)

            elif uow_record.state == unit_of_work.STATE_INVALID:
                self._process_state_invalid(freerun_entry, uow_record)

            elif uow_record.state == unit_of_work.STATE_CANCELED:
                self._process_state_canceled(freerun_entry, uow_record)

            else:
                msg = 'Unknown state %s of the unit_of_work %s' % (uow_record.state, uow_record.document['_id'])
                self._log_message(ERROR, freerun_entry, msg)

        except LookupError as e:
            msg = 'Lookup issue for schedulable: %r in timeperiod %s, because of: %r' \
                  % (freerun_entry.document['_id'], uow_record.timeperiod, e)
            self._log_message(WARNING, freerun_entry, msg)

    @with_reconnect
    def insert_uow(self, process_name, freerun_entry):
        """ creates unit_of_work and inserts it into the DB
            @raise DuplicateKeyError if unit_of_work with given parameters already exists """
        current_timeperiod = time_helper.actual_timeperiod(QUALIFIER_REAL_TIME)

        uow = UnitOfWork()
        uow.timeperiod = current_timeperiod
        uow.start_id = 0
        uow.end_id = 0
        uow.start_timeperiod = current_timeperiod
        uow.end_timeperiod = current_timeperiod
        uow.created_at = datetime.utcnow()
        uow.source = None
        uow.sink = None
        uow.state = unit_of_work.STATE_REQUESTED
        uow.process_name = process_name
        uow.number_of_retries = 0
        uow_id = self.uow_dao.insert(uow)

        mq_request = WorkerMqRequest()
        mq_request.process_name = process_name
        mq_request.unit_of_work_id = uow_id

        publisher = self.publishers.get(process_name)
        publisher.publish(mq_request.document)
        publisher.release()

        msg = 'Published: UOW %r for %r in timeperiod %r.' % (uow_id, process_name, current_timeperiod)
        self._log_message(INFO, freerun_entry, msg)
        return uow

    def _process_state_embryo(self, freerun_entry):
        """ method creates unit_of_work and associates it with the SchedulerFreerunEntry """
        process_name = '%s::%s' % (freerun_entry.process_name, freerun_entry.entry_name)
        try:
            uow = self.insert_uow(process_name, freerun_entry)
        except DuplicateKeyError as e:
            msg = 'Catching up with latest unit_of_work for %s, due to: %r' % (process_name, e)
            self._log_message(WARNING, freerun_entry, msg)
            uow = self.recover_from_duplicatekeyerror(e)

        if uow is not None:
            freerun_entry.related_unit_of_work = uow.document['_id']
            self.sfe_dao.update(freerun_entry)
        else:
            msg = 'MANUAL INTERVENTION REQUIRED! Unable to locate unit_of_work for %s' % process_name
            self._log_message(WARNING, freerun_entry, msg)

    def _process_state_requested(self, freerun_entry, uow_record):
        """ method that takes care of processing unit_of_work records in STATE_REQUESTED state"""



        time_qualifier = ProcessContext.get_time_qualifier(process_name)
        end_timeperiod = time_helper.increment_timeperiod(time_qualifier, start_timeperiod)

        try:
            uow = self.insert_uow(process_name, start_timeperiod, end_timeperiod, 0, job_record)
        except DuplicateKeyError as e:
            msg = 'Catching up with latest unit_of_work %s in timeperiod %s, because of: %r' \
                  % (process_name, job_record.timeperiod, e)
            self._log_message(WARNING, process_name, job_record, msg)
            uow = self.recover_from_duplicatekeyerror(e)

        if uow is not None:
            self.timetable.update_job_record(process_name, job_record, uow, job.STATE_IN_PROGRESS)
        else:
            msg = 'MANUAL INTERVENTION REQUIRED! Unable to locate unit_of_work for %s in %s' \
                  % (process_name, job_record.timeperiod)
            self._log_message(WARNING, process_name, job_record, msg)

    def _process_state_in_progress(self, freerun_entry, uow_record):
        """ method that takes care of processing job records in STATE_IN_PROGRESS state"""
        time_qualifier = ProcessContext.get_time_qualifier(process_name)
        end_timeperiod = time_helper.increment_timeperiod(time_qualifier, start_timeperiod)
        actual_timeperiod = time_helper.actual_timeperiod(time_qualifier)
        can_finalize_job_record = self.timetable.can_finalize_job_record(process_name, job_record)
        uow = self.uow_dao.get_one(job_record.related_unit_of_work)
        iteration = int(uow.end_id)

        try:
            if start_timeperiod == actual_timeperiod or can_finalize_job_record is False:
                if uow.state in [unit_of_work.STATE_REQUESTED,
                                 unit_of_work.STATE_IN_PROGRESS,
                                 unit_of_work.STATE_INVALID]:
                    # Large Job processing takes more than 1 tick of Scheduler
                    # Let the Job processing complete - do no updates to Scheduler records
                    pass
                elif uow.state in [unit_of_work.STATE_PROCESSED,
                                   unit_of_work.STATE_CANCELED]:
                    # create new uow to cover new inserts
                    uow = self.insert_uow(process_name, start_timeperiod, end_timeperiod, iteration + 1, job_record)
                    self.timetable.update_job_record(process_name, job_record, uow, job.STATE_IN_PROGRESS)

            elif start_timeperiod < actual_timeperiod and can_finalize_job_record is True:
                if uow.state in [unit_of_work.STATE_REQUESTED,
                                 unit_of_work.STATE_IN_PROGRESS,
                                 unit_of_work.STATE_INVALID]:
                    # Large Job processing has not started yet
                    # Let the Job processing complete - do no updates to Scheduler records
                    pass
                elif uow.state in [unit_of_work.STATE_PROCESSED,
                                   unit_of_work.STATE_CANCELED]:
                    # create new uow for FINAL RUN
                    uow = self.insert_uow(process_name, start_timeperiod, end_timeperiod, iteration + 1, job_record)
                    self.timetable.update_job_record(process_name, job_record, uow, job.STATE_FINAL_RUN)
            else:
                msg = 'Job record %s has timeperiod from future %s vs current time %s' \
                      % (job_record.document['_id'], start_timeperiod, actual_timeperiod)
                self._log_message(ERROR, process_name, job_record, msg)

        except DuplicateKeyError as e:
            uow = self.recover_from_duplicatekeyerror(e)
            if uow is not None:
                self.timetable.update_job_record(process_name, job_record, uow, job_record.state)
            else:
                msg = 'MANUAL INTERVENTION REQUIRED! Unable to identify unit_of_work for %s in %s' \
                      % (process_name, job_record.timeperiod)
                self._log_message(ERROR, process_name, job_record, msg)

    def _process_state_canceled(self, freerun_entry, uow_record):
        """method takes care of processing job records in STATE_SKIPPED state"""
        msg = 'Skipping job record %s in timeperiod %s. Apparently its most current timeperiod as of %s UTC' \
              % (job_record.document['_id'], job_record.timeperiod, str(datetime.utcnow()))
        self._log_message(WARNING, process_name, job_record, msg)

    def _process_state_invalid(self, freerun_entry, uow_record):
        """method takes care of processing unit_of_work records in STATE_INVALID state"""
        msg = 'Skipping job record %s in timeperiod %s. Apparently its most current timeperiod as of %s UTC' \
              % (job_record.document['_id'], job_record.timeperiod, str(datetime.utcnow()))
        self._log_message(WARNING, process_name, job_record, msg)

    def _process_state_processed(self, freerun_entry, uow_record):
        """method takes care of processing job records in STATE_PROCESSED state"""
        msg = 'Unexpected state %s of job record %s' % (freerun_entry.state, freerun_entry.document['_id'])
        self._log_message(ERROR, freerun_entry, msg)
