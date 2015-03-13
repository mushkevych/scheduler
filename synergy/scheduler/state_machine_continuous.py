__author__ = 'Bohdan Mushkevych'

from datetime import datetime
from logging import ERROR, WARNING, INFO

from synergy.db.model import job
from synergy.db.model.job import Job
from synergy.db.model.unit_of_work import UnitOfWork
from synergy.db.manager import ds_manager
from synergy.conf import context
from synergy.system.decorator import with_reconnect
from synergy.system import time_helper
from synergy.scheduler.scheduler_constants import STATE_MACHINE_CONTINUOUS
from synergy.scheduler.abstract_state_machine import AbstractStateMachine


class StateMachineContinuous(AbstractStateMachine):
    """ Continuous State Machine re-run process for timeperiod A until A+1,
        then transfers the timeperiod A to STATE_FINAL_RUN """

    def __init__(self, logger, timetable):
        super(StateMachineContinuous, self).__init__(logger, timetable, name=STATE_MACHINE_CONTINUOUS)
        self.ds = ds_manager.ds_factory(self.logger)

    def __del__(self):
        super(StateMachineContinuous, self).__del__()

    def shallow_state_update(self, uow):
        tree = self.timetable.get_tree(uow.process_name)
        node = tree.get_node(uow.process_name, uow.timeperiod)
        job_record = node.job_record
        assert isinstance(job_record, Job)

        if not job_record.is_final_run:
            self.logger.info('Can not perform shallow status update for %s in timeperiod %s '
                             'since the job state is not STATE_FINAL_RUN' % (uow.process_name, uow.timeperiod))
            return
        self._process_state_final_run(job_record)

    @with_reconnect
    def update_scope_of_processing(self, process_name, uow, start_timeperiod, end_timeperiod):
        """method reads collection and refine slice upper bound for processing"""
        source_collection_name = uow.source
        last_object_id = self.ds.lowest_primary_key(source_collection_name, start_timeperiod, end_timeperiod)
        uow.end_id = str(last_object_id)
        self.uow_dao.update(uow)

        msg = 'Updated range to process for %s in timeperiod %s for collection %s: [%s : %s]' \
              % (process_name, start_timeperiod, source_collection_name,
                 uow.start_id, str(last_object_id))
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
        uow, is_duplicate = self.insert_and_publish_uow(process_name,
                                                        start_timeperiod,
                                                        end_timeperiod,
                                                        start_id,
                                                        end_id)
        self.timetable.update_job_record(job_record, uow, job.STATE_IN_PROGRESS)

    def _compute_and_transfer_to_final_run(self, process_name, start_timeperiod, end_timeperiod, job_record):
        """ method computes new unit_of_work and transfers the job to STATE_FINAL_RUN
        it also shares _fuzzy_ DuplicateKeyError logic from _compute_and_transfer_to_progress method"""
        source_collection_name = context.process_context[process_name].source
        start_id = self.ds.highest_primary_key(source_collection_name, start_timeperiod, end_timeperiod)
        end_id = self.ds.lowest_primary_key(source_collection_name, start_timeperiod, end_timeperiod)
        uow, transfer_to_final = self.insert_and_publish_uow(process_name,
                                                             start_timeperiod,
                                                             end_timeperiod,
                                                             start_id,
                                                             end_id)
        self.timetable.update_job_record(job_record, uow, job.STATE_FINAL_RUN)

        if transfer_to_final:
            self._process_state_final_run(job_record)

    def _process_state_embryo(self, job_record):
        """ method that takes care of processing job records in STATE_EMBRYO state"""
        time_qualifier = context.process_context[job_record.process_name].time_qualifier
        end_timeperiod = time_helper.increment_timeperiod(time_qualifier, job_record.timeperiod)
        self._compute_and_transfer_to_progress(job_record.process_name, job_record.timeperiod,
                                               end_timeperiod, job_record)

    def _process_state_in_progress(self, job_record):
        """ method that takes care of processing job records in STATE_IN_PROGRESS state"""
        time_qualifier = context.process_context[job_record.process_name].time_qualifier
        end_timeperiod = time_helper.increment_timeperiod(time_qualifier, job_record.timeperiod)
        actual_timeperiod = time_helper.actual_timeperiod(time_qualifier)
        is_job_finalizable = self.timetable.is_job_record_finalizable(job_record)
        uow = self.uow_dao.get_one(job_record.related_unit_of_work)

        if job_record.timeperiod == actual_timeperiod or is_job_finalizable is False:
            if uow.is_invalid or uow.is_requested:
                # current uow has not been processed yet. update it
                self.update_scope_of_processing(job_record.process_name, uow, job_record.timeperiod, end_timeperiod)
            else:
                # STATE_IN_PROGRESS, STATE_PROCESSED, STATE_CANCELED
                # create new uow to cover new inserts
                self._compute_and_transfer_to_progress(job_record.process_name, job_record.timeperiod,
                                                       end_timeperiod, job_record)

        elif job_record.timeperiod < actual_timeperiod and is_job_finalizable is True:
            # create new uow for FINAL RUN
            self._compute_and_transfer_to_final_run(job_record.process_name, job_record.timeperiod,
                                                    end_timeperiod, job_record)

        else:
            msg = 'job record %s has timeperiod from future %s vs current time %s' \
                  % (job_record.db_id, job_record.timeperiod, actual_timeperiod)
            self._log_message(ERROR, job_record.process_name, job_record.timeperiod, msg)

    def _process_state_final_run(self, job_record):
        """method takes care of processing job records in STATE_FINAL_RUN state"""
        uow = self.uow_dao.get_one(job_record.related_unit_of_work)
        assert isinstance(uow, UnitOfWork)

        if uow.is_processed:
            self.timetable.update_job_record(job_record, uow, job.STATE_PROCESSED)
        elif uow.is_canceled:
            self.timetable.update_job_record(job_record, uow, job.STATE_SKIPPED)
        else:
            msg = 'Suppressed creating uow for %s in timeperiod %s; job record is in %s; uow is in %s' \
                  % (job_record.process_name, job_record.timeperiod, job_record.state, uow.state)
            self._log_message(INFO, job_record.process_name, job_record.timeperiod, msg)

        timetable_tree = self.timetable.get_tree(job_record.process_name)
        timetable_tree.build_tree()

    def _process_state_skipped(self, job_record):
        """method takes care of processing job records in STATE_SKIPPED state"""
        msg = 'Skipping job record %s in timeperiod %s. Apparently its most current timeperiod as of %s UTC' \
              % (job_record.db_id, job_record.timeperiod, str(datetime.utcnow()))
        self._log_message(WARNING, job_record.process_name, job_record.timeperiod, msg)

    def _process_state_processed(self, job_record):
        """method takes care of processing job records in STATE_PROCESSED state"""
        msg = 'Unexpected state %s of job record %s' % (job_record.state, job_record.db_id)
        self._log_message(ERROR, job_record.process_name, job_record.timeperiod, msg)
