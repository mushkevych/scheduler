__author__ = 'Bohdan Mushkevych'

from datetime import datetime
from logging import ERROR, WARNING, INFO

from synergy.db.error import DuplicateKeyError
from synergy.db.model import job, unit_of_work
from synergy.db.manager import ds_manager
from synergy.conf.process_context import ProcessContext
from synergy.system.decorator import with_reconnect
from synergy.system import time_helper
from synergy.scheduler.scheduler_constants import PIPELINE_CONTINUOUS
from synergy.scheduler.abstract_pipeline import AbstractPipeline


class ContinuousPipeline(AbstractPipeline):
    """ Continuous State Machine re-run process for timeperiod A until A+1, and the transfers the timeperiod to
     STATE_FINAL_RUN """

    def __init__(self, logger, timetable):
        super(ContinuousPipeline, self).__init__(logger, timetable, name=PIPELINE_CONTINUOUS)
        self.ds = ds_manager.ds_factory(self.logger)

    def __del__(self):
        super(ContinuousPipeline, self).__del__()

    @with_reconnect
    def update_scope_of_processing(self, process_name, uow, start_timeperiod, end_timeperiod, job_record):
        """method reads collection and refine slice upper bound for processing"""
        source_collection_name = uow.source
        last_object_id = self.ds.lowest_primary_key(source_collection_name, start_timeperiod, end_timeperiod)
        uow.end_id = str(last_object_id)
        self.uow_dao.update(uow)

        msg = 'Updated range to process for %s in timeperiod %s for collection %s: [%s : %s]' \
              % (process_name, job_record.timeperiod, source_collection_name,
                 uow.start_id, str(last_object_id))
        self._log_message(INFO, process_name, job_record, msg)

    def _compute_and_transfer_to_progress(self, process_name, start_timeperiod, end_timeperiod, job_record):
        """ method computes new unit_of_work for job record in STATE_IN_PROGRESS
        it also contains _fuzzy_ logic regard the DuplicateKeyError:
        - we try to compute new scope of processing
        - in case we face DuplicateKeyError, we try to recover from it by reading existing unit_of_work from DB:
        -- in case unit_of_work can be located - we update job record and proceed normally
        -- in case unit_of_work can not be located (what is equal to fatal data corruption) - we log exception and
        ask/expect manual intervention to resolve the corruption"""
        try:
            source_collection_name = ProcessContext.get_source(process_name)
            start_id = self.ds.highest_primary_key(source_collection_name, start_timeperiod, end_timeperiod)
            end_id = self.ds.lowest_primary_key(source_collection_name, start_timeperiod, end_timeperiod)
            uow_obj = self.create_and_publish_uow(process_name,
                                                  start_timeperiod,
                                                  end_timeperiod,
                                                  start_id,
                                                  end_id,
                                                  job_record)
        except DuplicateKeyError as e:
            uow_obj = self.recover_from_duplicatekeyerror(e)
            msg = 'No new data to process by %s in timeperiod %s, because of: %r' \
                  % (process_name, job_record.timeperiod, e)
            self._log_message(WARNING, process_name, job_record, msg)

        if uow_obj is not None:
            # we need to read existing uow from DB and make sure it is referenced by job_record
            self.timetable.update_job_record(process_name, job_record, uow_obj, job.STATE_IN_PROGRESS)
        else:
            msg = 'MANUAL INTERVENTION REQUIRED! Unable to locate unit_of_work for %s in %s' \
                  % (process_name, job_record.timeperiod)
            self._log_message(WARNING, process_name, job_record, msg)

    def _compute_and_transfer_to_final_run(self, process_name, start_timeperiod, end_timeperiod, job_record):
        """ method computes new unit_of_work and transfers the job to STATE_FINAL_RUN
        it also shares _fuzzy_ DuplicateKeyError logic from _compute_and_transfer_to_progress method"""
        transfer_to_final = False
        try:
            source_collection_name = ProcessContext.get_source(process_name)
            start_id = self.ds.highest_primary_key(source_collection_name, start_timeperiod, end_timeperiod)
            end_id = self.ds.lowest_primary_key(source_collection_name, start_timeperiod, end_timeperiod)
            uow_obj = self.create_and_publish_uow(process_name,
                                                  start_timeperiod,
                                                  end_timeperiod,
                                                  start_id,
                                                  end_id,
                                                  job_record)
        except DuplicateKeyError as e:
            transfer_to_final = True
            uow_obj = self.recover_from_duplicatekeyerror(e)

        if uow_obj is not None:
            self.timetable.update_job_record(process_name, job_record, uow_obj, job.STATE_FINAL_RUN)
        else:
            msg = 'MANUAL INTERVENTION REQUIRED! Unable to locate unit_of_work for %s in %s' \
                  % (process_name, job_record.timeperiod)
            self._log_message(WARNING, process_name, job_record, msg)

        if transfer_to_final:
            self._process_state_final_run(process_name, job_record)

    def _process_state_embryo(self, process_name, job_record, start_timeperiod):
        """ method that takes care of processing job records in STATE_EMBRYO state"""
        time_qualifier = ProcessContext.get_time_qualifier(process_name)
        end_timeperiod = time_helper.increment_timeperiod(time_qualifier, start_timeperiod)
        self._compute_and_transfer_to_progress(process_name, start_timeperiod, end_timeperiod, job_record)

    def _process_state_in_progress(self, process_name, job_record, start_timeperiod):
        """ method that takes care of processing job records in STATE_IN_PROGRESS state"""
        time_qualifier = ProcessContext.get_time_qualifier(process_name)
        end_timeperiod = time_helper.increment_timeperiod(time_qualifier, start_timeperiod)
        actual_timeperiod = time_helper.actual_timeperiod(time_qualifier)
        can_finalize_job_record = self.timetable.can_finalize_job_record(process_name, job_record)
        uow = self.uow_dao.get_one(job_record.related_unit_of_work)

        if start_timeperiod == actual_timeperiod or can_finalize_job_record is False:
            if uow.state in [unit_of_work.STATE_INVALID, unit_of_work.STATE_REQUESTED]:
                # current uow has not been processed yet. update it
                self.update_scope_of_processing(process_name, uow, start_timeperiod, end_timeperiod, job_record)
            else:
                # cls.STATE_IN_PROGRESS, cls.STATE_PROCESSED, cls.STATE_CANCELED
                # create new uow to cover new inserts
                self._compute_and_transfer_to_progress(process_name, start_timeperiod, end_timeperiod, job_record)

        elif start_timeperiod < actual_timeperiod and can_finalize_job_record is True:
            # create new uow for FINAL RUN
            self._compute_and_transfer_to_final_run(process_name, start_timeperiod, end_timeperiod, job_record)

        else:
            msg = 'job record %s has timeperiod from future %s vs current time %s' \
                  % (job_record.db_id, start_timeperiod, actual_timeperiod)
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
