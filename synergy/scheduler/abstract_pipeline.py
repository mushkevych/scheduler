__author__ = 'Bohdan Mushkevych'

from datetime import datetime
from logging import INFO, WARNING, ERROR

from synergy.db.model import job
from synergy.db.error import DuplicateKeyError
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.dao.job_dao import JobDao
from synergy.db.model import unit_of_work
from synergy.db.model.unit_of_work import UnitOfWork
from synergy.db.model.synergy_mq_transmission import SynergyMqTransmission
from synergy.mq.flopsy import PublishersPool
from synergy.conf.process_context import ProcessContext
from synergy.system.decorator import with_reconnect
from synergy.scheduler.scheduler_constants import TYPE_MANAGED


class AbstractPipeline(object):
    """ Abstract state machine used to govern all processes and their states """

    def __init__(self, logger, timetable, name):
        self.name = name
        self.logger = logger
        self.publishers = PublishersPool(self.logger)
        self.timetable = timetable
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.job_dao = JobDao(self.logger)

    def __del__(self):
        try:
            self.logger.info('Closing Flopsy Publishers Pool...')
            self.publishers.close()
        except Exception as e:
            self.logger.error('Exception caught while closing Flopsy Publishers Pool: %s' % str(e))

    def _log_message(self, level, process_name, timeperiod, msg):
        """ method performs logging into log file and Timetable's tree node"""
        self.timetable.add_log_entry(process_name, timeperiod, msg)
        self.logger.log(level, msg)

    @with_reconnect
    def _insert_uow(self, process_name, start_timeperiod, end_timeperiod, start_id, end_id):
        """creates unit_of_work and inserts it into the DB
            :raise DuplicateKeyError: if unit_of_work with given parameters already exists """
        uow = UnitOfWork()
        uow.process_name = process_name
        uow.timeperiod = start_timeperiod
        uow.start_id = str(start_id)
        uow.end_id = str(end_id)
        uow.start_timeperiod = start_timeperiod
        uow.end_timeperiod = end_timeperiod
        uow.created_at = datetime.utcnow()
        uow.source = ProcessContext.get_source(process_name)
        uow.sink = ProcessContext.get_sink(process_name)
        uow.state = unit_of_work.STATE_REQUESTED
        uow.unit_of_work_type = TYPE_MANAGED
        uow.number_of_retries = 0
        uow.arguments = ProcessContext.get_arguments(process_name)
        uow.document['_id'] = self.uow_dao.insert(uow)

        msg = 'Created: UOW %s for %s in timeperiod [%s:%s).' \
              % (uow.db_id, process_name, start_timeperiod, end_timeperiod)
        self._log_message(INFO, process_name, start_timeperiod, msg)
        return uow

    def _publish_uow(self, uow):
        mq_request = SynergyMqTransmission()
        mq_request.process_name = uow.process_name
        mq_request.unit_of_work_id = uow.db_id

        publisher = self.publishers.get(uow.process_name)
        publisher.publish(mq_request.document)
        publisher.release()

        msg = 'Published: UOW %r for %r in timeperiod %r.' % (uow.db_id, uow.process_name, uow.start_timeperiod)
        self._log_message(INFO, uow.process_name, uow.start_timeperiod, msg)

    def insert_and_publish_uow(self, process_name, start_timeperiod, end_timeperiod, start_id, end_id):
        """ method creates and publishes a unit_of_work. it also handles DuplicateKeyError and attempts recovery
        :return: tuple (uow, is_duplicate)
        :raise UserWarning: if the recovery from DuplicateKeyError was unsuccessful
        """
        is_duplicate = False
        try:
            uow = self._insert_uow(process_name, start_timeperiod, end_timeperiod, start_id, end_id)
        except DuplicateKeyError as e:
            is_duplicate = True
            msg = 'Catching up with latest unit_of_work %s in timeperiod %s, because of: %r' \
                  % (process_name, start_timeperiod, e)
            self._log_message(WARNING, process_name, start_timeperiod, msg)
            uow = self.uow_dao.recover_from_duplicatekeyerror(e)

        if uow is None:
            msg = 'MANUAL INTERVENTION REQUIRED! Unable to locate unit_of_work for %s in %s' \
                  % (process_name, start_timeperiod)
            self._log_message(WARNING, process_name, start_timeperiod, msg)
            raise UserWarning(msg)

        # publish the created/caught up unit_of_work
        self._publish_uow(uow)
        return uow, is_duplicate

    def shallow_state_update(self, uow):
        """ method does not trigger any new actions
        if applicable, it will update job_record state and Timetable tree node state
        :assumptions: uow is either in STATE_CANCELED or STATE_PROCESSED """
        pass

    def _process_state_embryo(self, process_name, job_record, start_timeperiod):
        """ method that takes care of processing job records in STATE_EMBRYO state"""
        pass

    def _process_state_in_progress(self, process_name, job_record, start_timeperiod):
        """ method that takes care of processing job records in STATE_IN_PROGRESS state"""
        pass

    def _process_state_final_run(self, process_name, job_record):
        """method takes care of processing job records in STATE_FINAL_RUN state"""
        pass

    def _process_state_skipped(self, process_name, job_record):
        """method takes care of processing job records in STATE_FINAL_SKIPPED state"""
        pass

    def _process_state_processed(self, process_name, job_record):
        """method takes care of processing job records in STATE_FINAL_SKIPPED state"""
        pass

    def manage_pipeline_with_blocking_children(self, process_name, job_record):
        """ method will trigger job processing only if all children are in STATE_PROCESSED or STATE_SKIPPED
         and if all external dependencies are finalized (i.e. in STATE_PROCESSED or STATE_SKIPPED) """
        green_light = self.timetable.can_finalize_job_record(process_name, job_record)
        if green_light:
            self.manage_pipeline_for_process(process_name, job_record)
        else:
            msg = '%s for timeperiod %r is blocked by unprocessed children/dependencies. Waiting another tick' \
                  % (process_name, job_record.timeperiod)
            self._log_message(INFO, process_name, job_record.timeperiod, msg)

    def manage_pipeline_with_blocking_dependencies(self, process_name, job_record):
        """ method will trigger job processing only if _all_ dependencies are in STATE_PROCESSED
         method will transfer current job into STATE_SKIPPED if any dependency is in STATE_SKIPPED """
        all_finalized, all_processed, skipped_present = self.timetable.is_dependent_on_finalized(process_name,
                                                                                                 job_record)
        if all_processed:
            self.manage_pipeline_for_process(process_name, job_record)
        elif skipped_present:
            # As soon as among <dependent on> periods are in STATE_SKIPPED
            # there is very little sense in waiting for them to become STATE_PROCESSED
            # Skip this timeperiod itself
            job_record.state = job.STATE_SKIPPED
            self.job_dao.update(job_record)
            tree = self.timetable.get_tree(process_name)
            tree.update_node_by_process(process_name, job_record)

            msg = '%s for timeperiod %r is blocked by STATE_SKIPPED dependencies. ' \
                  'Transferred the job to STATE_SKIPPED' % (process_name, job_record.timeperiod)
            self._log_message(WARNING, process_name, job_record.timeperiod, msg)
        else:
            msg = '%s for timeperiod %r is blocked by unprocessed dependencies. Waiting another tick' \
                  % (process_name, job_record.timeperiod)
            self._log_message(INFO, process_name, job_record.timeperiod, msg)

    def manage_pipeline_for_process(self, process_name, job_record):
        """ method main duty - is to _avoid_ publishing another unit_of_work, if previous was not yet processed
        In case the Scheduler sees that the unit_of_work is pending it could either update boundaries of the processing
        or wait another tick """
        try:
            if job_record.state == job.STATE_EMBRYO:
                self._process_state_embryo(process_name, job_record, job_record.timeperiod)

            elif job_record.state == job.STATE_IN_PROGRESS:
                self._process_state_in_progress(process_name, job_record, job_record.timeperiod)

            elif job_record.state == job.STATE_FINAL_RUN:
                self._process_state_final_run(process_name, job_record)

            elif job_record.state == job.STATE_SKIPPED:
                self._process_state_skipped(process_name, job_record)

            elif job_record.state == job.STATE_PROCESSED:
                self._process_state_processed(process_name, job_record)

            else:
                msg = 'Unknown state %s of the job %s' % (job_record.state, job_record.db_id)
                self._log_message(ERROR, process_name, job_record.timeperiod, msg)

        except LookupError as e:
            self.timetable.failed_on_processing_job_record(process_name, job_record.timeperiod)
            msg = 'Increasing fail counter for %s in timeperiod %s, because of: %r' \
                  % (process_name, job_record.timeperiod, e)
            self._log_message(WARNING, process_name, job_record.timeperiod, msg)
