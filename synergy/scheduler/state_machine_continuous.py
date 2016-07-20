__author__ = 'Bohdan Mushkevych'

from logging import ERROR

from synergy.db.model import job
from synergy.scheduler.scheduler_constants import STATE_MACHINE_CONTINUOUS
from synergy.scheduler.abstract_state_machine import AbstractStateMachine
from synergy.system import time_helper
from synergy.conf import context


class StateMachineContinuous(AbstractStateMachine):
    """ State Machine of 6 states is expected to spawn multiple UOWs per job/timeperiod.
        Job timeperiods boundaries are meant to be discrete/fixed
        in comparison to StateMachineRecomputing this one does not re-compute processing boundaries """

    def __init__(self, logger, timetable, name=STATE_MACHINE_CONTINUOUS):
        super(StateMachineContinuous, self).__init__(logger, timetable, name)

    @property
    def run_on_active_timeperiod(self):
        """ :return: True, as we allow multiple runs on a given timeperiod """
        return True

    def notify(self, uow):
        tree = self.timetable.get_tree(uow.process_name)
        node = tree.get_node(uow.process_name, uow.timeperiod)
        job_record = node.job_record

        if not job_record.is_final_run:
            self.logger.info('Suppressing state change for Job {0}@{1}, since it is not in STATE_FINAL_RUN'
                             .format(uow.process_name, uow.timeperiod))
            return
        self._process_state_final_run(job_record)
        self.mq_transmitter.publish_job_status(job_record)

    def _compute_next_job_state(self, job_record):
        time_qualifier = context.process_context[job_record.process_name].time_qualifier
        actual_timeperiod = time_helper.actual_timeperiod(time_qualifier)
        is_job_finalizable = self.timetable.is_job_record_finalizable(job_record)

        if job_record.timeperiod == actual_timeperiod or is_job_finalizable is False:
            return job.STATE_IN_PROGRESS

        elif job_record.timeperiod < actual_timeperiod and is_job_finalizable is True:
            return job.STATE_FINAL_RUN

        else:
            msg = 'Job {0} has timeperiod {1} from the future vs current timeperiod {2}' \
                .format(job_record.db_id, job_record.timeperiod, actual_timeperiod)
            self._log_message(ERROR, job_record.process_name, job_record.timeperiod, msg)
            raise ValueError(msg)

    def _process_state_embryo(self, job_record):
        """ method that takes care of processing job records in STATE_EMBRYO state"""
        uow, is_duplicate = self.insert_and_publish_uow(job_record, 0, 0)
        try:
            target_state = self._compute_next_job_state(job_record)
            self.update_job(job_record, uow, target_state)
        except ValueError:
            # do no processing for the future timeperiods
            pass

    def _process_state_in_progress(self, job_record):
        """ method that takes care of processing job records in STATE_IN_PROGRESS state"""
        def _process_state(target_state, uow):
            if uow.is_active:
                # Large Job processing takes more than 1 tick of the Scheduler
                # Let the Job processing complete - do no updates to Scheduler records
                pass
            elif uow.is_finished:
                # create new UOW to cover new inserts
                new_uow, is_duplicate = self.insert_and_publish_uow(job_record, 0, int(uow.end_id) + 1)
                self.update_job(job_record, new_uow, target_state)

        uow = self.uow_dao.get_one(job_record.related_unit_of_work)
        try:
            target_state = self._compute_next_job_state(job_record)
            _process_state(target_state, uow)
        except ValueError:
            # do no processing for the future timeperiods
            pass
