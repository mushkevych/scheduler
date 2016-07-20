__author__ = 'Bohdan Mushkevych'

from logging import ERROR, INFO

from synergy.db.model import job
from synergy.db.manager import ds_manager
from synergy.conf import context
from synergy.system.decorator import with_reconnect
from synergy.system import time_helper
from synergy.scheduler.scheduler_constants import STATE_MACHINE_RECOMPUTING
from synergy.scheduler.abstract_state_machine import AbstractStateMachine


class StateMachineRecomputing(AbstractStateMachine):
    """ State Machine of 6 states is expected to spawn multiple UOWs per job/timeperiod.
        Job timeperiods boundaries are dynamic - i.e. underlying data volume grows with time """

    def __init__(self, logger, timetable):
        super(StateMachineRecomputing, self).__init__(logger, timetable, name=STATE_MACHINE_RECOMPUTING)
        self.ds = ds_manager.ds_factory(self.logger)

    @property
    def run_on_active_timeperiod(self):
        """ :return: True, as this StateMachine allows multiple runs for some given timeperiod """
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

    @with_reconnect
    def update_scope_of_processing(self, process_name, uow, start_timeperiod, end_timeperiod):
        """method reads collection and refine slice upper bound for processing"""
        source_collection_name = uow.source
        last_object_id = self.ds.lowest_primary_key(source_collection_name, start_timeperiod, end_timeperiod)
        uow.end_id = str(last_object_id)
        self.uow_dao.update(uow)

        msg = 'Updated processing range for {0}@{1} for collection {2}: [{3} : {4}]' \
              .format(process_name, start_timeperiod, source_collection_name, uow.start_id, uow.end_id)
        self._log_message(INFO, process_name, start_timeperiod, msg)

    def _compute_and_transfer_to_progress(self, process_name, start_timeperiod, end_timeperiod, job_record):
        """ method computes new unit_of_work for job record in STATE_IN_PROGRESS
        it also contains _fuzzy_ logic regard the DuplicateKeyError:
        - we try to compute new scope of processing
        - in case we face DuplicateKeyError, we try to recover from it by reading existing unit_of_work from DB:
        -- in case unit_of_work can be located - we update job record and proceed normally
        -- in case unit_of_work can not be located (what is equal to fatal data corruption) - we log exception and
        ask/expect manual intervention to resolve the corruption"""
        source_collection_name = context.process_context[process_name].source
        start_id = self.ds.highest_primary_key(source_collection_name, start_timeperiod, end_timeperiod)
        end_id = self.ds.lowest_primary_key(source_collection_name, start_timeperiod, end_timeperiod)
        uow, is_duplicate = self.insert_and_publish_uow(job_record, start_id, end_id)
        self.update_job(job_record, uow, job.STATE_IN_PROGRESS)

    def _compute_and_transfer_to_final_run(self, process_name, start_timeperiod, end_timeperiod, job_record):
        """ method computes new unit_of_work and transfers the job to STATE_FINAL_RUN
        it also shares _fuzzy_ DuplicateKeyError logic from _compute_and_transfer_to_progress method"""
        source_collection_name = context.process_context[process_name].source
        start_id = self.ds.highest_primary_key(source_collection_name, start_timeperiod, end_timeperiod)
        end_id = self.ds.lowest_primary_key(source_collection_name, start_timeperiod, end_timeperiod)
        uow, transfer_to_final = self.insert_and_publish_uow(job_record, start_id, end_id)
        self.update_job(job_record, uow, job.STATE_FINAL_RUN)

        if transfer_to_final:
            self._process_state_final_run(job_record)

    def _process_state_embryo(self, job_record):
        """ method that takes care of processing job records in STATE_EMBRYO state"""
        start_timeperiod = self.compute_start_timeperiod(job_record.process_name, job_record.timeperiod)
        end_timeperiod = self.compute_end_timeperiod(job_record.process_name, job_record.timeperiod)
        self._compute_and_transfer_to_progress(job_record.process_name, start_timeperiod,
                                               end_timeperiod, job_record)

    def _process_state_in_progress(self, job_record):
        """ method that takes care of processing job records in STATE_IN_PROGRESS state"""
        start_timeperiod = self.compute_start_timeperiod(job_record.process_name, job_record.timeperiod)
        end_timeperiod = self.compute_end_timeperiod(job_record.process_name, job_record.timeperiod)

        time_qualifier = context.process_context[job_record.process_name].time_qualifier
        actual_timeperiod = time_helper.actual_timeperiod(time_qualifier)

        is_job_finalizable = self.timetable.is_job_record_finalizable(job_record)
        uow = self.uow_dao.get_one(job_record.related_unit_of_work)

        if job_record.timeperiod == actual_timeperiod or is_job_finalizable is False:
            if uow.is_invalid or uow.is_requested:
                # current uow has not been processed yet. update it
                self.update_scope_of_processing(job_record.process_name, uow, start_timeperiod, end_timeperiod)
            else:
                # STATE_IN_PROGRESS, STATE_PROCESSED, STATE_CANCELED, STATE_NOOP
                # create new uow to cover new inserts
                self._compute_and_transfer_to_progress(job_record.process_name, start_timeperiod,
                                                       end_timeperiod, job_record)

        elif job_record.timeperiod < actual_timeperiod and is_job_finalizable is True:
            # create new uow for FINAL RUN
            self._compute_and_transfer_to_final_run(job_record.process_name, start_timeperiod,
                                                    end_timeperiod, job_record)

        else:
            msg = 'Job {0} has timeperiod {1} from the future vs current timeperiod {2}' \
                  .format(job_record.db_id, job_record.timeperiod, actual_timeperiod)
            self._log_message(ERROR, job_record.process_name, job_record.timeperiod, msg)
