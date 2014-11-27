__author__ = 'Bohdan Mushkevych'

from logging import ERROR, INFO

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
        tree = self.timetable.get_tree(uow.process_name)
        node = tree.get_node_by_process(uow.process_name, uow.timeperiod)

        job_record = node.job_record
        if job_record.state != job.STATE_IN_PROGRESS:
            self.logger.info('Can not perform shallow status update for %s in timeperiod %s '
                             'since the job state is not STATE_IN_PROGRESS' % (uow.process_name, uow.timeperiod))
            return

        time_qualifier = ProcessContext.get_time_qualifier(uow.process_name)
        actual_timeperiod = time_helper.actual_timeperiod(time_qualifier)
        can_finalize_job_record = self.timetable.can_finalize_job_record(uow.process_name, job_record)

        if uow.timeperiod < actual_timeperiod and can_finalize_job_record is True:
            self.__process_finalizable_job(uow.process_name, job_record, uow)

        elif uow.timeperiod >= actual_timeperiod:
            self.logger.info('Can not complete shallow status update for %s in timeperiod %s '
                             'since the working timeperiod has not finished yet' % (uow.process_name, uow.timeperiod))

        elif not can_finalize_job_record:
            self.logger.info('Can not complete shallow status update for %s in timeperiod %s '
                             'since the job could not be finalized' % (uow.process_name, uow.timeperiod))

    def __process_non_finalizable_job(self, process_name, job_record, uow, start_timeperiod, end_timeperiod):
        """ method handles given job_record based on the unit_of_work status
        Assumption: job_record is in STATE_IN_PROGRESS and is not yet finalizable """
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
                                                            int(uow.end_id) + 1)
            self.timetable.update_job_record(process_name, job_record, uow, job.STATE_IN_PROGRESS)

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
            self._log_message(INFO, process_name, job_record.timeperiod, msg)
        elif uow.state == unit_of_work.STATE_PROCESSED:
            self.timetable.update_job_record(process_name, job_record, uow, job.STATE_PROCESSED)
        elif uow.state == unit_of_work.STATE_CANCELED:
            self.timetable.update_job_record(process_name, job_record, uow, job.STATE_SKIPPED)
        else:
            msg = 'Unknown state %s for job record %s in timeperiod %s for %s' \
                  % (uow.state, job_record.db_id, job_record.timeperiod, process_name)
            self._log_message(INFO, process_name, job_record.timeperiod, msg)

        timetable_tree = self.timetable.get_tree(process_name)
        timetable_tree.build_tree()

    def _process_state_in_progress(self, process_name, job_record, start_timeperiod):
        """ method that takes care of processing job records in STATE_IN_PROGRESS state """
        time_qualifier = ProcessContext.get_time_qualifier(process_name)
        end_timeperiod = time_helper.increment_timeperiod(time_qualifier, start_timeperiod)
        actual_timeperiod = time_helper.actual_timeperiod(time_qualifier)
        can_finalize_job_record = self.timetable.can_finalize_job_record(process_name, job_record)
        uow = self.uow_dao.get_one(job_record.related_unit_of_work)

        if start_timeperiod == actual_timeperiod or can_finalize_job_record is False:
            self.__process_non_finalizable_job(process_name, job_record, uow, start_timeperiod, end_timeperiod)

        elif start_timeperiod < actual_timeperiod and can_finalize_job_record is True:
            self.__process_finalizable_job(process_name, job_record, uow)

        else:
            msg = 'Job record %s has timeperiod from future %s vs current time %s' \
                  % (job_record.db_id, start_timeperiod, actual_timeperiod)
            self._log_message(ERROR, process_name, job_record.timeperiod, msg)

    def _process_state_final_run(self, process_name, job_record):
        """method takes care of processing job records in STATE_FINAL_RUN state"""
        raise NotImplementedError('Method _process_state_final_run is not supported by %s' % self.__class__.__name__)
