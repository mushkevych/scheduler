__author__ = 'Bohdan Mushkevych'

from logging import ERROR, INFO

from synergy.db.model import job
from synergy.scheduler.scheduler_constants import STATE_MACHINE_SIMPLE_DISCRETE
from synergy.scheduler.state_machine_dicrete import StateMachineDiscrete
from synergy.system import time_helper
from synergy.conf import context


class StateMachineSimpleDiscrete(StateMachineDiscrete):
    """ State Machine to handle discrete timeperiod boundaries for jobs
    in comparison to StateMachineDiscrete this one does not transfer to STATE_FINAL_RUN"""

    def __init__(self, logger, timetable):
        super(StateMachineSimpleDiscrete, self).__init__(logger, timetable, name=STATE_MACHINE_SIMPLE_DISCRETE)

    def __del__(self):
        super(StateMachineSimpleDiscrete, self).__del__()

    def shallow_state_update(self, uow):
        tree = self.timetable.get_tree(uow.process_name)
        node = tree.get_node(uow.process_name, uow.timeperiod)

        job_record = node.job_record
        if not job_record.is_in_progress:
            self.logger.info('Can not perform shallow status update for %s in timeperiod %s '
                             'since the job state is not STATE_IN_PROGRESS' % (uow.process_name, uow.timeperiod))
            return

        time_qualifier = context.process_context[uow.process_name].time_qualifier
        actual_timeperiod = time_helper.actual_timeperiod(time_qualifier)
        is_job_finalizable = self.timetable.is_job_record_finalizable(job_record)

        if uow.timeperiod < actual_timeperiod and is_job_finalizable is True:
            self.__process_finalizable_job(job_record, uow)

        elif uow.timeperiod >= actual_timeperiod:
            self.logger.info('Can not complete shallow status update for %s in timeperiod %s '
                             'since the working timeperiod has not finished yet' % (uow.process_name, uow.timeperiod))

        elif not is_job_finalizable:
            self.logger.info('Can not complete shallow status update for %s in timeperiod %s '
                             'since the job could not be finalized' % (uow.process_name, uow.timeperiod))

    def __process_non_finalizable_job(self, job_record, uow, start_timeperiod, end_timeperiod):
        """ method handles given job_record based on the unit_of_work status
        Assumption: job_record is in STATE_IN_PROGRESS and is not yet finalizable """
        if uow.is_active:
            # Large Job processing takes more than 1 tick of Scheduler
            # Let the Large Job processing complete - do no updates to Scheduler records
            pass
        elif uow.is_finished:
            # create new uow to cover new inserts
            uow, is_duplicate = self.insert_and_publish_uow(job_record.process_name,
                                                            start_timeperiod,
                                                            end_timeperiod,
                                                            0,
                                                            int(uow.end_id) + 1)
            self.timetable.update_job_record(job_record, uow, job.STATE_IN_PROGRESS)

    def __process_finalizable_job(self, job_record, uow):
        """ method handles given job_record based on the unit_of_work status
        Assumption: job_record is in STATE_IN_PROGRESS and is finalizable """
        if uow.is_active:
            # Job processing has not started yet
            # Let the processing complete - do no updates to Scheduler records
            msg = 'Suppressed creating uow for %s in timeperiod %s; job record is in %s; uow is in %s' \
                  % (job_record.process_name, job_record.timeperiod, job_record.state, uow.state)
            self._log_message(INFO, job_record.process_name, job_record.timeperiod, msg)
        elif uow.is_processed:
            self.timetable.update_job_record(job_record, uow, job.STATE_PROCESSED)
        elif uow.is_canceled:
            self.timetable.update_job_record(job_record, uow, job.STATE_SKIPPED)
        else:
            msg = 'Unknown state %s for job record %s in timeperiod %s for %s' \
                  % (uow.state, job_record.db_id, job_record.timeperiod, job_record.process_name)
            self._log_message(INFO, job_record.process_name, job_record.timeperiod, msg)

        timetable_tree = self.timetable.get_tree(job_record.process_name)
        timetable_tree.build_tree()

    def _process_state_in_progress(self, job_record):
        """ method that takes care of processing job records in STATE_IN_PROGRESS state """
        time_qualifier = context.process_context[job_record.process_name].time_qualifier
        end_timeperiod = time_helper.increment_timeperiod(time_qualifier, job_record.timeperiod)
        actual_timeperiod = time_helper.actual_timeperiod(time_qualifier)
        is_job_finalizable = self.timetable.is_job_record_finalizable(job_record)
        uow = self.uow_dao.get_one(job_record.related_unit_of_work)

        if job_record.timeperiod == actual_timeperiod or is_job_finalizable is False:
            self.__process_non_finalizable_job(job_record, uow, job_record.timeperiod, end_timeperiod)

        elif job_record.timeperiod < actual_timeperiod and is_job_finalizable is True:
            self.__process_finalizable_job(job_record, uow)

        else:
            msg = 'Job record %s has timeperiod from future %s vs current time %s' \
                  % (job_record.db_id, job_record.timeperiod, actual_timeperiod)
            self._log_message(ERROR, job_record.process_name, job_record.timeperiod, msg)

    def _process_state_final_run(self, job_record):
        """method takes care of processing job records in STATE_FINAL_RUN state"""
        raise NotImplementedError('Method _process_state_final_run is not supported by %s' % self.__class__.__name__)
