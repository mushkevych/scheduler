__author__ = 'Bohdan Mushkevych'

from logging import ERROR, INFO, WARNING

from synergy.db.model import job, unit_of_work
from synergy.scheduler.scheduler_constants import PIPELINE_SIMPLIFIED_DISCRETE
from synergy.scheduler.dicrete_pipeline import DiscretePipeline
from synergy.system import time_helper
from synergy.conf.process_context import ProcessContext


class SimplifiedDiscretePipeline(DiscretePipeline):
    """ Pipeline to handle discrete timeperiod boundaries for jobs
    in comparison to DiscretePipeline this one does not transfer to STATE_FINAL_RUN"""

    def __init__(self, logger, timetable):
        super(SimplifiedDiscretePipeline, self).__init__(logger, timetable, name=PIPELINE_SIMPLIFIED_DISCRETE)

    def __del__(self):
        super(SimplifiedDiscretePipeline, self).__del__()

    def shallow_state_update(self, uow):
        if uow.state not in [unit_of_work.STATE_PROCESSED, unit_of_work.STATE_CANCELED]:
            # rely on Garbage Collector to re-trigger the failing unit_of_work
            return

        tree = self.timetable.get_tree(uow.process_name)
        node = tree.get_node_by_process(uow.process_name, uow.timeperiod)

        job_record = node.job_record
        if job_record.state != job.STATE_IN_PROGRESS:
            return

        time_qualifier = ProcessContext.get_time_qualifier(uow.process_name)
        actual_timeperiod = time_helper.actual_timeperiod(time_qualifier)
        can_finalize_job_record = self.timetable.can_finalize_job_record(uow.process_name, job_record)

        if uow.timeperiod < actual_timeperiod and can_finalize_job_record is True:
            self.__process_finalizable_job(uow.process_name, job_record, uow)

        elif uow.timeperiod >= actual_timeperiod:
            msg = 'Can not complete shallow status update for %s in timeperiod %s ' \
                  'since the working timeperiod has not finished yet' % (uow.process_name, uow.timeperiod)
            self._log_message(WARNING, uow.process_name, job_record, msg)

        elif not can_finalize_job_record:
            msg = 'Can not complete shallow status update for %s in timeperiod %s ' \
                  'since the job could not be finalized' % (uow.process_name, uow.timeperiod)
            self._log_message(WARNING, uow.process_name, job_record, msg)

    def __process_finalizable_job(self, process_name, job_record, uow):
        """ method handles given job_record based on the unit_of_work status
        Assumption: job_record is in STATE_IN_PROGRESS and is finalizable """

        if uow.state in [unit_of_work.STATE_REQUESTED,
                         unit_of_work.STATE_IN_PROGRESS,
                         unit_of_work.STATE_INVALID]:
            # Job processing has not started yet
            # Let the processing complete - do no updates to Scheduler records
            msg = 'Suppressed creating uow for %s in timeperiod %s; job record is in %s; uow is in %s' \
                  % (process_name, job_record.timeperiod, job_record.state, uow.state)
        elif uow.state == unit_of_work.STATE_PROCESSED:
            self.timetable.update_job_record(process_name, job_record, uow, job.STATE_PROCESSED)
            msg = 'Transferred job record %s in timeperiod %s to STATE_PROCESSED for %s' \
                  % (job_record.db_id, job_record.timeperiod, process_name)
        elif uow.state == unit_of_work.STATE_CANCELED:
            self.timetable.update_job_record(process_name, job_record, uow, job.STATE_SKIPPED)
            msg = 'Transferred job record %s in timeperiod %s to STATE_SKIPPED for %s' \
                  % (job_record.db_id, job_record.timeperiod, process_name)
        else:
            msg = 'Unknown state %s for job record %s in timeperiod %s for %s' \
                  % (uow.state, job_record.db_id, job_record.timeperiod, process_name)

        timetable_tree = self.timetable.get_tree(process_name)
        timetable_tree.build_tree()
        self._log_message(INFO, process_name, job_record, msg)

    def _process_state_in_progress(self, process_name, job_record, start_timeperiod):
        """ method that takes care of processing job records in STATE_IN_PROGRESS state """
        time_qualifier = ProcessContext.get_time_qualifier(process_name)
        end_timeperiod = time_helper.increment_timeperiod(time_qualifier, start_timeperiod)
        actual_timeperiod = time_helper.actual_timeperiod(time_qualifier)
        can_finalize_job_record = self.timetable.can_finalize_job_record(process_name, job_record)
        uow = self.uow_dao.get_one(job_record.related_unit_of_work)
        iteration = int(uow.end_id)

        if start_timeperiod == actual_timeperiod or can_finalize_job_record is False:
            if uow.state in [unit_of_work.STATE_REQUESTED,
                             unit_of_work.STATE_IN_PROGRESS,
                             unit_of_work.STATE_INVALID]:
                # Large Job processing takes more than 1 tick of Scheduler
                # Let the Large Job processing complete - do no updates to Scheduler records
                pass
            elif uow.state in [unit_of_work.STATE_PROCESSED,
                               unit_of_work.STATE_CANCELED]:
                # create new uow to cover new inserts
                uow, is_duplicate = self.insert_and_publish_uow(process_name,
                                                                start_timeperiod,
                                                                end_timeperiod,
                                                                0,
                                                                iteration + 1,
                                                                job_record)
                self.timetable.update_job_record(process_name, job_record, uow, job.STATE_IN_PROGRESS)

        elif start_timeperiod < actual_timeperiod and can_finalize_job_record is True:
            self.__process_finalizable_job(process_name, job_record, uow)

        else:
            msg = 'Job record %s has timeperiod from future %s vs current time %s' \
                  % (job_record.db_id, start_timeperiod, actual_timeperiod)
            self._log_message(ERROR, process_name, job_record, msg)

    def _process_state_final_run(self, process_name, job_record):
        """method takes care of processing job records in STATE_FINAL_RUN state"""
        raise NotImplementedError('Method _process_state_final_run is not supported by SimplifiedDiscretePipeline')
