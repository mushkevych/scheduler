__author__ = 'Bohdan Mushkevych'

from pymongo import ASCENDING, DESCENDING
from datetime import datetime
from logging import ERROR, WARNING, INFO

from abstract_pipeline import AbstractPipeline
from db.error import DuplicateKeyError
from db.model import base_model, time_table_record, unit_of_work
from db.model.unit_of_work import UnitOfWork
from system.decorator import with_reconnect
from system.process_context import ProcessContext
from system.collection_context import CollectionContext
from system import time_helper


class RegularPipeline(AbstractPipeline):
    """ Regular pipeline triggers Python-based aggregators """

    def __init__(self, scheduler, timetable):
        super(RegularPipeline, self).__init__(scheduler, timetable)

    def __del__(self):
        super(RegularPipeline, self).__del__()

    @with_reconnect
    def compute_scope_of_processing(self, process_name, start_time, end_time, timetable_record):
        """method reads collection and identify slice for processing"""
        source_collection_name = ProcessContext.get_source_collection(process_name)
        target_collection_name = ProcessContext.get_target_collection(process_name)
        source_collection = CollectionContext.get_collection(self.logger, source_collection_name)

        query = {base_model.TIMEPERIOD: {'$gte': start_time, '$lt': end_time}}
        asc_search = source_collection.find(spec=query, fields='_id').sort('_id', ASCENDING).limit(1)
        if asc_search.count() == 0:
            raise LookupError('No messages in timeperiod: %s:%s in collection %s'
                              % (start_time, end_time, source_collection_name))
        first_object_id = asc_search[0]['_id']

        dec_search = source_collection.find(spec=query, fields='_id').sort('_id', DESCENDING).limit(1)
        last_object_id = dec_search[0]['_id']

        uow = UnitOfWork()
        uow.timeperiod = start_time
        uow.start_id = str(first_object_id)
        uow.end_id = str(last_object_id)
        uow.start_timeperiod = start_time
        uow.end_timeperiod = end_time
        uow.created_at = datetime.utcnow()
        uow.source_collection = source_collection_name
        uow.target_collection = target_collection_name
        uow.state = unit_of_work.STATE_REQUESTED
        uow.process_name = process_name
        uow.number_of_retries = 0

        try:
            uow_id = self.uow_dao.insert(uow)
        except DuplicateKeyError as e:
            e.first_object_id = str(first_object_id)
            e.last_object_id = str(last_object_id)
            e.process_name = process_name
            e.timeperiod = start_time
            raise e

        self.publishers.get_publisher(process_name).publish(str(uow_id))
        msg = 'Published: UOW %r for %r in timeperiod %r.' % (uow_id, process_name, start_time)
        self._log_message(INFO, process_name, timetable_record, msg)
        return uow

    @with_reconnect
    def update_scope_of_processing(self, process_name, unit_of_work, start_time, end_time, timetable_record):
        """method reads collection and refine slice upper bound for processing"""
        source_collection_name = unit_of_work.source_collection
        source_collection = CollectionContext.get_collection(self.logger, source_collection_name)

        query = {base_model.TIMEPERIOD: {'$gte': start_time, '$lt': end_time}}
        dec_search = source_collection.find(spec=query, fields='_id').sort('_id', DESCENDING).limit(1)
        last_object_id = dec_search[0]['_id']
        unit_of_work.end_id = str(last_object_id)
        self.uow_dao.update(unit_of_work)

        msg = 'Updated range to process for %s in timeperiod %s for collection %s: [%s : %s]' \
              % (process_name, timetable_record.timeperiod, source_collection_name,
                 unit_of_work.start_id, str(last_object_id))
        self._log_message(INFO, process_name, timetable_record, msg)

    def _compute_and_transfer_to_progress(self, process_name, start_time, end_time, timetable_record):
        """ method computes new unit_of_work for time-table-record in STATE_IN_PROGRESS
        it also contains _fuzzy_ logic regard the DuplicateKeyError:
        - we try to compute new scope of processing
        - in case we face DuplicateKeyError, we try to recover from it by reading existing unit_of_work from DB:
        -- in case units of work can be located - we update timetable_record, and proceed normally
        -- in case unit_of_work can not be located (what is equal to fatal data corruption) - we log exception and
        expect for manual intervention"""
        try:
            uow_obj = self.compute_scope_of_processing(process_name, start_time, end_time, timetable_record)
        except DuplicateKeyError as e:
            uow_obj = self.recover_from_duplicatekeyerror(e)
            msg = 'No new data to process by %s in timeperiod %s, because of: %r' \
                  % (process_name, timetable_record.timeperiod, e)
            self._log_message(WARNING, process_name, timetable_record, msg)

        if uow_obj is not None:
            # we need to read existing uow from DB and make sure it is referenced by timetable_record
            self.timetable.update_timetable_record(process_name,
                                                   timetable_record,
                                                   uow_obj,
                                                   time_table_record.STATE_IN_PROGRESS)
        else:
            msg = 'MANUAL INTERVENTION REQUIRED! Unable to locate unit_of_work for %s in %s' \
                  % (process_name, timetable_record.timeperiod)
            self._log_message(WARNING, process_name, timetable_record, msg)

    def _compute_and_transfer_to_final_run(self, process_name, start_time, end_time, timetable_record):
        """ method computes new unit_of_work and transfers timeperiod to STATE_FINAL_RUN
        it also shares _fuzzy_ DuplicateKeyError logic from _compute_and_transfer_to_progress method"""
        transfer_to_final = False
        try:
            uow_obj = self.compute_scope_of_processing(process_name, start_time, end_time, timetable_record)
        except DuplicateKeyError as e:
            transfer_to_final = True
            uow_obj = self.recover_from_duplicatekeyerror(e)

        if uow_obj is not None:
            self.timetable.update_timetable_record(process_name,
                                                   timetable_record,
                                                   uow_obj,
                                                   time_table_record.STATE_FINAL_RUN)
        else:
            msg = 'MANUAL INTERVENTION REQUIRED! Unable to locate unit_of_work for %s in %s' \
                  % (process_name, timetable_record.timeperiod)
            self._log_message(WARNING, process_name, timetable_record, msg)

        if transfer_to_final:
            self._process_state_final_run(process_name, timetable_record)

    def _process_state_embryo(self, process_name, timetable_record, start_time):
        """ method that takes care of processing timetable records in STATE_EMBRYO state"""
        end_time = time_helper.increment_time(process_name, start_time)
        self._compute_and_transfer_to_progress(process_name, start_time, end_time, timetable_record)

    def _process_state_in_progress(self, process_name, timetable_record, start_time):
        """ method that takes care of processing timetable records in STATE_IN_PROGRESS state"""
        end_time = time_helper.increment_time(process_name, start_time)
        actual_time = time_helper.actual_time(process_name)
        can_finalize_timerecord = self.timetable.can_finalize_timetable_record(process_name, timetable_record)
        uow = self.uow_dao.get_one(timetable_record.related_unit_of_work)

        if start_time == actual_time or can_finalize_timerecord is False:
            if uow.state in [unit_of_work.STATE_INVALID,
                             unit_of_work.STATE_REQUESTED]:
                # current uow has not been processed yet. update it
                self.update_scope_of_processing(process_name, uow, start_time, end_time, timetable_record)
            else:
                # cls.STATE_IN_PROGRESS, cls.STATE_PROCESSED, cls.STATE_CANCELED
                # create new uow to cover new inserts
                self._compute_and_transfer_to_progress(process_name, start_time, end_time, timetable_record)

        elif start_time < actual_time and can_finalize_timerecord is True:
            # create new uow for FINAL RUN
            self._compute_and_transfer_to_final_run(process_name, start_time, end_time, timetable_record)

        else:
            msg = 'time-table-record %s has timeperiod from future %s vs current time %s' \
                  % (timetable_record.document['_id'], start_time, actual_time)
            self._log_message(ERROR, process_name, timetable_record, msg)

    def _process_state_final_run(self, process_name, timetable_record):
        """method takes care of processing timetable records in STATE_FINAL_RUN state"""
        uow = self.uow_dao.get_one(timetable_record.related_unit_of_work)

        if uow.state == unit_of_work.STATE_PROCESSED:
            self.timetable.update_timetable_record(process_name,
                                                   timetable_record,
                                                   uow,
                                                   time_table_record.STATE_PROCESSED)
            timetable_tree = self.timetable.get_tree(process_name)
            timetable_tree.build_tree()
            msg = 'Transferred time-table-record %s in timeperiod %s to STATE_PROCESSED for %s' \
                  % (timetable_record.document['_id'], timetable_record.timeperiod, process_name)
        elif uow.state == unit_of_work.STATE_CANCELED:
            self.timetable.update_timetable_record(process_name,
                                                   timetable_record,
                                                   uow,
                                                   time_table_record.STATE_SKIPPED)
            msg = 'Transferred time-table-record %s in timeperiod %s to STATE_SKIPPED for %s' \
                  % (timetable_record.document['_id'], timetable_record.timeperiod, process_name)
        else:
            msg = 'Suppressed creating uow for %s in timeperiod %s; timetable_record is in %s; uow is in %s' \
                  % (process_name, timetable_record.timeperiod, timetable_record.state, uow.state)
        self._log_message(INFO, process_name, timetable_record, msg)

    def _process_state_skipped(self, process_name, timetable_record):
        """method takes care of processing timetable records in STATE_SKIPPED state"""
        msg = 'Skipping time-table-record %s in timeperiod %s. Apparently its most current timeperiod as of %s UTC' \
              % (timetable_record.document['_id'], timetable_record.timeperiod, str(datetime.utcnow()))
        self._log_message(WARNING, process_name, timetable_record, msg)

    def _process_state_processed(self, process_name, timetable_record):
        """method takes care of processing timetable records in STATE_PROCESSED state"""
        msg = 'Unexpected state %s of time-table-record %s' % (timetable_record.state,
                                                               timetable_record.document['_id'])
        self._log_message(ERROR, process_name, timetable_record, msg)
