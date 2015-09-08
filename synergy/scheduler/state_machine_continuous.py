__author__ = 'Bohdan Mushkevych'

from logging import INFO

from synergy.db.model import job
from synergy.db.manager import ds_manager
from synergy.conf import context
from synergy.system.decorator import with_reconnect
from synergy.system import time_helper
from synergy.scheduler.scheduler_constants import STATE_MACHINE_CONTINUOUS
from synergy.scheduler.state_machine_dicrete import StateMachineDiscrete


class StateMachineContinuous(StateMachineDiscrete):
    """ Continuous State Machine re-run process for timeperiod T until T+1,
        then transfers the timeperiod A to STATE_FINAL_RUN """

    def __init__(self, logger, timetable):
        super(StateMachineContinuous, self).__init__(logger, timetable, name=STATE_MACHINE_CONTINUOUS)
        self.ds = ds_manager.ds_factory(self.logger)

    def __del__(self):
        super(StateMachineContinuous, self).__del__()

    @with_reconnect
    def _update_processing_scope(self, process_name, uow, start_timeperiod, end_timeperiod):
        """method reads collection and refine upper boundary for processing"""
        source_collection_name = uow.source
        last_object_id = self.ds.lowest_primary_key(source_collection_name, start_timeperiod, end_timeperiod)
        uow.end_id = str(last_object_id)
        self.uow_dao.update(uow)

        msg = 'Updated range to process for {0} in timeperiod {1} for collection {2}: [{3} : {4}]' \
              .format(process_name, start_timeperiod, source_collection_name, uow.start_id, uow.end_id)
        self._log_message(INFO, process_name, start_timeperiod, msg)

    def _compute_processing_scope(self, process_name, start_timeperiod, end_timeperiod, job_record, target_state):
        """ method computes new unit_of_work and transfers the job record into **target_state**
            it also contains _fuzzy_ logic regard the DuplicateKeyError:
            - we try to compute new scope of processing
            - in case we face DuplicateKeyError, we try to recover from it by reading existing unit_of_work from DB:
            -- in case unit_of_work can be located - we update job record and proceed normally
            -- in case unit_of_work can not be located (what is equal to fatal data corruption) - we log exception and
            ask/expect manual intervention to resolve the corruption"""
        source_collection_name = context.process_context[process_name].source
        start_id = self.ds.highest_primary_key(source_collection_name, start_timeperiod, end_timeperiod)
        end_id = self.ds.lowest_primary_key(source_collection_name, start_timeperiod, end_timeperiod)
        uow, is_duplicate = self.insert_and_publish_uow(process_name,
                                                        start_timeperiod,
                                                        end_timeperiod,
                                                        start_id,
                                                        end_id)
        self.update_job(job_record, uow, target_state)

        if is_duplicate and target_state == job.STATE_FINAL_RUN:
            self._process_state_final_run(job_record)

    def _process_state_embryo(self, job_record):
        """ method that takes care of processing job records in STATE_EMBRYO state"""
        time_qualifier = context.process_context[job_record.process_name].time_qualifier
        end_timeperiod = time_helper.increment_timeperiod(time_qualifier, job_record.timeperiod)

        try:
            target_state = self._compute_next_job_state(job_record)
            self._compute_processing_scope(job_record.process_name, job_record.timeperiod,
                                           end_timeperiod, job_record, target_state)
        except ValueError:
            # do no processing for the future timeperiods
            pass

    def _process_state_in_progress(self, job_record):
        """ method that takes care of processing job records in STATE_IN_PROGRESS state"""
        time_qualifier = context.process_context[job_record.process_name].time_qualifier
        end_timeperiod = time_helper.increment_timeperiod(time_qualifier, job_record.timeperiod)
        uow = self.uow_dao.get_one(job_record.related_unit_of_work)

        try:
            target_state = self._compute_next_job_state(job_record)

            if target_state == job.STATE_IN_PROGRESS and (uow.is_invalid or uow.is_requested):
                # current UOW has not been processed yet. updating its db representation with new boundaries
                self._update_processing_scope(job_record.process_name, uow, job_record.timeperiod, end_timeperiod)
            else:
                # STATE_IN_PROGRESS, STATE_PROCESSED, STATE_CANCELED, STATE_NOOP
                # create new uow to cover new inserts
                self._compute_processing_scope(job_record.process_name, job_record.timeperiod,
                                               end_timeperiod, job_record, target_state)
        except ValueError:
            # do no processing for the future timeperiods
            pass
