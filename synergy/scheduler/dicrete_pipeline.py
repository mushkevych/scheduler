__author__ = 'Bohdan Mushkevych'

from datetime import datetime
from logging import ERROR, WARNING, INFO

from synergy.db.error import DuplicateKeyError
from synergy.db.model import job, unit_of_work
from synergy.scheduler.scheduler_constants import PIPELINE_DISCRETE
from synergy.scheduler.abstract_pipeline import AbstractPipeline
from synergy.system import time_helper
from synergy.conf.process_context import ProcessContext


class DiscretePipeline(AbstractPipeline):
    """ Pipeline to handle discrete timeperiod boundaries for batch jobs
    in comparison to RegularPipeline this one does not re-compute processing boundaries"""

    def __init__(self, logger, timetable, name=PIPELINE_DISCRETE):
        super(DiscretePipeline, self).__init__(logger, timetable, name)

    def __del__(self):
        super(DiscretePipeline, self).__del__()

    def _process_state_embryo(self, process_name, job_record, start_timeperiod):
        """ method that takes care of processing job records in STATE_EMBRYO state"""
        time_qualifier = ProcessContext.get_time_qualifier(process_name)
        end_timeperiod = time_helper.increment_timeperiod(time_qualifier, start_timeperiod)

        try:
            uow = self.create_and_publish_uow(process_name, start_timeperiod, end_timeperiod, 0, 0, job_record)
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

    def _process_state_in_progress(self, process_name, job_record, start_timeperiod):
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
                    uow = self.create_and_publish_uow(process_name, start_timeperiod, end_timeperiod,
                                                      0, iteration + 1, job_record)
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
                    uow = self.create_and_publish_uow(process_name, start_timeperiod, end_timeperiod,
                                                      0, iteration + 1, job_record)
                    self.timetable.update_job_record(process_name, job_record, uow, job.STATE_FINAL_RUN)
            else:
                msg = 'Job record %s has timeperiod from future %s vs current time %s' \
                      % (job_record.db_id, start_timeperiod, actual_timeperiod)
                self._log_message(ERROR, process_name, job_record, msg)

        except DuplicateKeyError as e:
            uow = self.recover_from_duplicatekeyerror(e)
            if uow is not None:
                self.timetable.update_job_record(process_name, job_record, uow, job_record.state)
            else:
                msg = 'MANUAL INTERVENTION REQUIRED! Unable to identify unit_of_work for %s in %s' \
                      % (process_name, job_record.timeperiod)
                self._log_message(ERROR, process_name, job_record, msg)

    def _process_state_final_run(self, process_name, job_record):
        """method takes care of processing job records in STATE_FINAL_RUN state"""
        uow = self.uow_dao.get_one(job_record.related_unit_of_work)

        if uow.state == unit_of_work.STATE_PROCESSED:
            self.timetable.update_job_record(process_name, job_record, uow, job.STATE_PROCESSED)
            timetable_tree = self.timetable.get_tree(process_name)
            timetable_tree.build_tree()
            msg = 'Transferred job record %s in timeperiod %s to STATE_PROCESSED for %s' \
                  % (job_record.db_id, job_record.timeperiod, process_name)
        elif uow.state == unit_of_work.STATE_CANCELED:
            self.timetable.update_job_record(process_name, job_record, uow, job.STATE_SKIPPED)
            msg = 'Transferred job record %s in timeperiod %s to STATE_SKIPPED for %s' \
                  % (job_record.db_id, job_record.timeperiod, process_name)
        else:
            msg = 'Suppressed creating uow for %s in timeperiod %s; job record is in %s; uow is in %s' \
                  % (process_name, job_record.timeperiod, job_record.state, uow.state)
        self._log_message(INFO, process_name, job_record, msg)

    def _process_state_skipped(self, process_name, job_record):
        """method takes care of processing job records in STATE_SKIPPED state"""
        msg = 'Skipping job record %s in timeperiod %s. Apparently its most current timeperiod as of %s UTC' \
              % (job_record.db_id, job_record.timeperiod, str(datetime.utcnow()))
        self._log_message(WARNING, process_name, job_record, msg)

    def _process_state_processed(self, process_name, job_record):
        """method takes care of processing job records in STATE_PROCESSED state"""
        msg = 'Unexpected state %s of job record %s' % (job_record.state, job_record.db_id)
        self._log_message(ERROR, process_name, job_record, msg)
