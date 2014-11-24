__author__ = 'Bohdan Mushkevych'

from logging import ERROR, INFO, WARNING

from synergy.db.error import DuplicateKeyError
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

    def shallow_state_update(self, process_name, timeperiod, uow_state):
        if uow_state != unit_of_work.STATE_PROCESSED:
            # rely on Garbage Collector to re-trigger the failing unit_of_work
            return

        try:
            tree = self.timetable.get_tree(process_name)
            node = tree.get_node_by_process(process_name, timeperiod)
            job_record = node.job_record

            if job_record.state in [job.STATE_EMBRYO, job.STATE_SKIPPED, job.STATE_PROCESSED]:
                # nothing to do
                pass

            elif job_record.state == job.STATE_IN_PROGRESS:
                time_qualifier = ProcessContext.get_time_qualifier(process_name)
                actual_timeperiod = time_helper.actual_timeperiod(time_qualifier)
                can_finalize_job_record = self.timetable.can_finalize_job_record(process_name, job_record)
                uow = self.uow_dao.get_one(job_record.related_unit_of_work)
                if timeperiod < actual_timeperiod and can_finalize_job_record is True:
                    self.timetable.update_job_record(process_name, job_record, uow, job.STATE_PROCESSED)
                    timetable_tree = self.timetable.get_tree(process_name)
                    timetable_tree.build_tree()
                    msg = 'Transferred job record %s in timeperiod %s to STATE_PROCESSED for %s' \
                          % (job_record.db_id, job_record.timeperiod, process_name)
                    self._log_message(INFO, process_name, job_record, msg)

                elif timeperiod >= actual_timeperiod:
                    msg = 'Can not complete shallow status update for %s in timeperiod %s ' \
                          'since the working timeperiod has not finished yet' % (process_name, timeperiod)
                    self._log_message(WARNING, process_name, job_record, msg)

                elif not can_finalize_job_record:
                    msg = 'Can not complete shallow status update for %s in timeperiod %s ' \
                          'since the job could not be finalized' % (process_name, timeperiod)
                    self._log_message(WARNING, process_name, job_record, msg)

            else:
                msg = 'Unknown state %s of the job %s' % (job_record.state, job_record.db_id)
                self._log_message(ERROR, process_name, job_record, msg)

        except LookupError as e:
            self.logger.warning('Unable to perform shallow state update for %s in timeperiod %s, because of: %r'
                                % (process_name, timeperiod, e))

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
                    # Let the Large Job processing complete - do no updates to Scheduler records
                    pass
                elif uow.state in [unit_of_work.STATE_PROCESSED,
                                   unit_of_work.STATE_CANCELED]:
                    # create new uow to cover new inserts
                    uow = self.insert_uow(process_name, start_timeperiod, end_timeperiod, 0, iteration + 1, job_record)
                    self.publish_uow(job_record, uow)
                    self.timetable.update_job_record(process_name, job_record, uow, job.STATE_IN_PROGRESS)

            elif start_timeperiod < actual_timeperiod and can_finalize_job_record is True:
                if uow.state in [unit_of_work.STATE_REQUESTED,
                                 unit_of_work.STATE_IN_PROGRESS,
                                 unit_of_work.STATE_INVALID]:
                    # Job processing has not started yet
                    # Let the processing complete - do no updates to Scheduler records
                    msg = 'Suppressed creating uow for %s in timeperiod %s; job record is in %s; uow is in %s' \
                          % (process_name, job_record.timeperiod, job_record.state, uow.state)
                elif uow.state == unit_of_work.STATE_PROCESSED:
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
                    msg = 'Unknown state %s for job record %s in timeperiod %s for %s' \
                          % (uow.state, job_record.db_id, job_record.timeperiod, process_name)

                self._log_message(INFO, process_name, job_record, msg)
            else:
                msg = 'Job record %s has timeperiod from future %s vs current time %s' \
                      % (job_record.db_id, start_timeperiod, actual_timeperiod)
                self._log_message(ERROR, process_name, job_record, msg)

        except DuplicateKeyError as e:
            uow = self.uow_dao.recover_from_duplicatekeyerror(e)
            if uow is not None:
                self.publish_uow(job_record, uow)
                self.timetable.update_job_record(process_name, job_record, uow, job_record.state)
            else:
                msg = 'MANUAL INTERVENTION REQUIRED! Unable to identify unit_of_work for %s in %s' \
                      % (process_name, job_record.timeperiod)
                self._log_message(ERROR, process_name, job_record, msg)

    def _process_state_final_run(self, process_name, job_record):
        """method takes care of processing job records in STATE_FINAL_RUN state"""
        raise NotImplementedError('Method _process_state_final_run is not supported by SimplifiedDiscretePipeline')
