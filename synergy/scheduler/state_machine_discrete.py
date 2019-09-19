__author__ = 'Bohdan Mushkevych'

from logging import ERROR, INFO

from synergy.db.model import job
from synergy.scheduler.scheduler_constants import STATE_MACHINE_DISCRETE
from synergy.scheduler.abstract_state_machine import AbstractStateMachine
from synergy.system import time_helper
from synergy.conf import context


class StateMachineDiscrete(AbstractStateMachine):
    """ State Machine of 5 states is expected to spawn one UOW per job/timeperiod
        Job timeperiods boundaries are meant to be discrete/fixed
        in comparison to StateMachineContinuous this one does not transfer to STATE_FINAL_RUN"""

    def __init__(self, logger, timetable):
        super(StateMachineDiscrete, self).__init__(logger, timetable, name=STATE_MACHINE_DISCRETE)

    @property
    def run_on_active_timeperiod(self):
        """ :return: False, since there should be only 1 run for given timeperiod """
        return False

    def notify(self, uow):
        tree = self.timetable.get_tree(uow.process_name)
        node = tree.get_node(uow.process_name, uow.timeperiod)
        job_record = node.job_record

        if not job_record.is_in_progress:
            self.logger.info('Suppressing state change for Job {0}@{1}, since it is not in STATE_IN_PROGRESS'
                             .format(uow.process_name, uow.timeperiod))
            return

        time_qualifier = context.process_context[uow.process_name].time_qualifier
        actual_timeperiod = time_helper.actual_timeperiod(time_qualifier)
        is_job_finalizable = self.timetable.is_job_record_finalizable(job_record)

        if uow.timeperiod < actual_timeperiod and is_job_finalizable is True:
            self.__process_finalizable_job(job_record, uow)
            self.mq_transmitter.publish_job_status(job_record)

        elif uow.timeperiod >= actual_timeperiod:
            self.logger.info('Suppressing state change for Job {0}@{1}, '
                             'since the working timeperiod has not finished yet'
                             .format(uow.process_name, uow.timeperiod))

        elif not is_job_finalizable:
            self.logger.info('Suppressing state change for Job {0}@{1}, '
                             'since the job is not finalizable'.format(uow.process_name, uow.timeperiod))

    def __process_non_finalizable_job(self, job_record, uow):
        """ method handles given job_record based on the unit_of_work status
        Assumption: job_record is in STATE_IN_PROGRESS and is not yet finalizable """
        if uow.is_active:
            # Large Job processing takes more than 1 tick of Scheduler
            # Let the Large Job processing complete - do no updates to Scheduler records
            pass
        elif uow.is_finished:
            # create new uow to cover new inserts
            uow, is_duplicate = self.insert_and_publish_uow(job_record, 0, int(uow.end_id) + 1)
            self.update_job(job_record, uow, job.STATE_IN_PROGRESS)

    def __process_finalizable_job(self, job_record, uow):
        """ method handles given job_record based on the unit_of_work status
        Assumption: job_record is in STATE_IN_PROGRESS and is finalizable """
        if uow.is_active:
            # Job processing has not started yet
            # Let the processing complete - do no updates to Scheduler records
            msg = 'Suppressed new UOW creation for Job {0}@{1}; Job is in {2}; UOW is in {3}' \
                  .format(job_record.process_name, job_record.timeperiod, job_record.state, uow.state)
            self._log_message(INFO, job_record.process_name, job_record.timeperiod, msg)
        elif uow.is_processed:
            self.update_job(job_record, uow, job.STATE_PROCESSED)
        elif uow.is_noop:
            self.update_job(job_record, uow, job.STATE_NOOP)
        elif uow.is_canceled:
            self.update_job(job_record, uow, job.STATE_SKIPPED)
        elif uow.is_invalid:
            msg = 'Job {0}: UOW for {1}@{2} is in {3}; ' \
                  'relying on the Garbage Collector to transfer UOW into the STATE_CANCELED' \
                  .format(job_record.db_id, job_record.process_name, job_record.timeperiod, uow.state)
            self._log_message(INFO, job_record.process_name, job_record.timeperiod, msg)
        else:
            msg = 'Unknown state {0} of Job {1} for {2}@{3}' \
                  .format(uow.state, job_record.db_id, job_record.process_name, job_record.timeperiod)
            self._log_message(INFO, job_record.process_name, job_record.timeperiod, msg)

        timetable_tree = self.timetable.get_tree(job_record.process_name)
        timetable_tree.build_tree()

    def _process_state_embryo(self, job_record):
        """ method that takes care of processing job records in STATE_EMBRYO state"""
        uow, is_duplicate = self.insert_and_publish_uow(job_record, 0, 0)
        self.update_job(job_record, uow, job.STATE_IN_PROGRESS)

    def _process_state_in_progress(self, job_record):
        """ method that takes care of processing job records in STATE_IN_PROGRESS state """
        time_qualifier = context.process_context[job_record.process_name].time_qualifier
        actual_timeperiod = time_helper.actual_timeperiod(time_qualifier)
        is_job_finalizable = self.timetable.is_job_record_finalizable(job_record)
        uow = self.uow_dao.get_one(job_record.related_unit_of_work)

        if job_record.timeperiod == actual_timeperiod or is_job_finalizable is False:
            self.__process_non_finalizable_job(job_record, uow)

        elif job_record.timeperiod < actual_timeperiod and is_job_finalizable is True:
            self.__process_finalizable_job(job_record, uow)

        else:
            msg = 'Job {0} has timeperiod {1} from the future vs current timeperiod {2}' \
                  .format(job_record.db_id, job_record.timeperiod, actual_timeperiod)
            self._log_message(ERROR, job_record.process_name, job_record.timeperiod, msg)

    def _process_state_final_run(self, job_record):
        """method takes care of processing job records in STATE_FINAL_RUN state"""
        raise NotImplementedError(f'Method _process_state_final_run is not supported by {self.__class__.__name__}')
