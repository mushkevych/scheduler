"""
Created on 2011-02-07

@author: Bohdan Mushkevych
"""

from pymongo import ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError
from pymongo.objectid import ObjectId
from datetime import datetime
from logging import ERROR, WARNING, INFO

from model.abstract_model import AbstractModel
from abstract_pipeline import AbstractPipeline
from unit_of_work_entry import UnitOfWorkEntry
from time_table_entry import TimeTableEntry
from system.process_context import ProcessContext
from system.collection_context import CollectionContext, with_reconnect
from system import time_helper
import unit_of_work_helper


class RegularPipeline(AbstractPipeline):
    """ Scheduler encapsulate logic for starting the aggregators/alerts/googlereport reader """

    def __init__(self, scheduler, timetable):
        super(RegularPipeline, self).__init__(scheduler, timetable)


    def __del__(self):
        super(RegularPipeline, self).__del__()

    @with_reconnect
    def compute_scope_of_processing(self, process_name, start_time, end_time, time_record):
        """method reads collection and identify slice for processing"""
        source_collection_name = ProcessContext.get_source_collection(process_name)
        target_collection_name = ProcessContext.get_target_collection(process_name)
        source_collection = CollectionContext.get_collection(self.logger, source_collection_name)

        query = { AbstractModel.TIMESTAMP : { '$gte' : start_time, '$lt' : end_time } }
        asc_search = source_collection.find(spec=query, fields='_id').sort('_id', ASCENDING).limit(1)
        if asc_search.count() == 0:
            raise LookupError('No messages in timeperiod: %s:%s in collection %s'
                                % (start_time, end_time, source_collection_name))
        first_object_id = asc_search[0]['_id']

        dec_search = source_collection.find(spec=query, fields='_id').sort('_id', DESCENDING).limit(1)
        last_object_id = dec_search[0]['_id']

        unit_of_work = UnitOfWorkEntry()
        unit_of_work.set_timestamp(start_time)
        unit_of_work.set_start_id(str(first_object_id))
        unit_of_work.set_end_id(str(last_object_id))
        unit_of_work.set_start_timestamp(start_time)
        unit_of_work.set_end_timestamp(end_time)
        unit_of_work.set_created_at(datetime.utcnow())
        unit_of_work.set_source_collection(source_collection_name)
        unit_of_work.set_target_collection(target_collection_name)
        unit_of_work.set_state(unit_of_work.STATE_REQUESTED)
        unit_of_work.set_process_name(process_name)
        unit_of_work.set_number_of_retries(0)

        try:
            uow_id = unit_of_work_helper.insert(self.logger, unit_of_work)
        except DuplicateKeyError as e:
            e.first_object_id = str(first_object_id)
            e.last_object_id = str(last_object_id)
            e.process_name = process_name
            e.timestamp = start_time
            raise e

        self.publishers.get_publisher(process_name).publish(str(uow_id))
        msg = 'Published: UOW %r for %r in timeperiod %r.' % (uow_id, process_name, start_time)
        self._log_message(INFO, process_name, time_record, msg)
        return unit_of_work


    @with_reconnect
    def update_scope_of_processing(self, process_name, unit_of_work, start_time, end_time, time_record):
        """method reads collection and refine slice upper bound for processing"""
        source_collection_name = unit_of_work.get_source_collection()
        source_collection = CollectionContext.get_collection(self.logger, source_collection_name)

        query = { AbstractModel.TIMESTAMP : { '$gte' : start_time, '$lt' : end_time } }
        dec_search = source_collection.find(spec=query, fields='_id').sort('_id', DESCENDING).limit(1)
        last_object_id = dec_search[0]['_id']
        unit_of_work.set_end_id(str(last_object_id))
        unit_of_work_helper.update(self.logger, unit_of_work)

        msg = 'Updated range to process for %s in timeperiod %s for collection %s: [%s : %s]'\
                % (process_name, time_record.get_timestamp(), source_collection_name,
                   unit_of_work.get_start_id(), str(last_object_id))
        self._log_message(INFO, process_name, time_record, msg)


    def _compute_and_transfer_to_progress(self, process_name, start_time, end_time, time_record):
        """ method computes new unit_of_work for time-record in STATE_IN_PROGRESS
        it also contains _fuzzy_ logic regard the DuplicateKeyError:
        - we try to compute new scope of processing
        - in case we face DuplicateKeyError, we try to recover from it by reading existing unit_of_work from DB:
        -- in case units of work can be located - we update time_record, and proceed normally
        -- in case unit_of_work can not be located (what is equal to fatal data corruption) - we log exception and
        expect for manual intervention"""
        uow_obj = None
        try:
            uow_obj = self.compute_scope_of_processing(process_name, start_time, end_time, time_record)
        except DuplicateKeyError as e:
            uow_obj = self.recover_from_duplicatekeyerror(e)
            msg = 'No new data to process by %s in timeperiod %s, because of: %r'\
                    % (process_name, time_record.get_timestamp(), e)
            self._log_message(WARNING, process_name, time_record, msg)

        if uow_obj is not None:
            # we need to read existing uow from DB and make sure it is referenced by time_record
            self.timetable.update_timetable_record(process_name,
                                                   time_record,
                                                   uow_obj,
                                                   TimeTableEntry.STATE_IN_PROGRESS)
        else:
            msg = 'MANUAL INTERVENTION REQUIRED! Unable to locate unit_of_work for %s in %s' \
                    % (process_name, time_record.get_timestamp())
            self._log_message(WARNING, process_name, time_record, msg)


    def _compute_and_transfer_to_final_run(self, process_name, start_time, end_time, time_record):
        """ method computes new unit_of_work and transfers timeperiod to STATE_FINAL_RUN
        it also shares _fuzzy_ DuplicateKeyError logic from _compute_and_transfer_to_progress method"""
        transfer_to_final = False
        try:
            uow_obj = self.compute_scope_of_processing(process_name, start_time, end_time, time_record)
        except DuplicateKeyError as e:
            transfer_to_final = True
            uow_obj = self.recover_from_duplicatekeyerror(e)

        if uow_obj is not None:
            self.timetable.update_timetable_record(process_name,
                                                   time_record,
                                                   uow_obj,
                                                   TimeTableEntry.STATE_FINAL_RUN)
        else:
            msg = 'MANUAL INTERVENTION REQUIRED! Unable to locate unit_of_work for %s in %s' \
                    % (process_name, time_record.get_timestamp())
            self._log_message(WARNING, process_name, time_record, msg)

        if transfer_to_final:
            self._process_state_final_run(process_name, time_record)


    def _process_state_embryo(self, process_name, time_record, start_time):
        """ method that takes care of processing timetable records in STATE_EMBRYO state"""
        end_time = time_helper.increment_time(process_name, start_time)
        self._compute_and_transfer_to_progress(process_name, start_time, end_time, time_record)


    def _process_state_in_progress(self, process_name, time_record, start_time):
        """ method that takes care of processing timetable records in STATE_IN_PROGRESS state"""
        end_time = time_helper.increment_time(process_name, start_time)
        actual_time = time_helper.actual_time(process_name)
        can_finalize_timerecord = self.timetable.can_finalize_timetable_record(process_name, time_record)
        uow_id = time_record.get_related_unit_of_work()
        uow_obj = unit_of_work_helper.retrieve_by_id(self.logger, ObjectId(uow_id))

        if start_time == actual_time or can_finalize_timerecord == False:
            if uow_obj.get_state() == UnitOfWorkEntry.STATE_INVALID\
                or uow_obj.get_state() == UnitOfWorkEntry.STATE_REQUESTED:
                # current uow has not been processed yet. update it
                self.update_scope_of_processing(process_name, uow_obj, start_time, end_time, time_record)
            else:
                # cls.STATE_IN_PROGRESS, cls.STATE_PROCESSED, cls.STATE_CANCELED
                # create new uow to cover new inserts
                self._compute_and_transfer_to_progress(process_name, start_time, end_time, time_record)

        elif start_time < actual_time and can_finalize_timerecord == True:
            # create new uow for FINAL RUN
            self._compute_and_transfer_to_final_run(process_name, start_time, end_time, time_record)

        else:
            msg = 'Time-record %s has timestamp from future %s vs current time %s'\
                    % (time_record.get_document()['_id'], start_time, actual_time)
            self._log_message(ERROR, process_name, time_record, msg)


    def _process_state_final_run(self, process_name, time_record):
        """method takes care of processing timetable records in STATE_FINAL_RUN state"""
        uow_id = time_record.get_related_unit_of_work()
        uow_obj = unit_of_work_helper.retrieve_by_id(self.logger, ObjectId(uow_id))

        if uow_obj.get_state() == UnitOfWorkEntry.STATE_PROCESSED:
            self.timetable.update_timetable_record(process_name,
                                                   time_record,
                                                   uow_obj,
                                                   TimeTableEntry.STATE_PROCESSED)
            timetable_tree = self.timetable.get_tree(process_name)
            timetable_tree.build_tree()
            msg = 'Transferred time-record %s in timeperiod %s to STATE_PROCESSED for %s'\
                    % (time_record.get_document()['_id'], time_record.get_timestamp(), process_name)
        elif uow_obj.get_state() == UnitOfWorkEntry.STATE_CANCELED:
            self.timetable.update_timetable_record(process_name,
                                                   time_record,
                                                   uow_obj,
                                                   TimeTableEntry.STATE_SKIPPED)
            msg = 'Transferred time-record %s in timeperiod %s to STATE_SKIPPED for %s'\
                    % (time_record.get_document()['_id'], time_record.get_timestamp(), process_name)
        else:
            msg = 'Suppressed creating uow for %s in timeperiod %s; time_record is in %s; uow is in %s'\
                % (process_name, time_record.get_timestamp(), time_record.get_state(), uow_obj.get_state())
        self._log_message(INFO, process_name, time_record, msg)

        
    def _process_state_skipped(self, process_name, time_record):
        """method takes care of processing timetable records in STATE_SKIPPED state"""
        msg = 'Skipping time-record %s in timeperiod %s. Apparently its most current timeperiod as of %s UTC' \
                        % (time_record.get_document()['_id'], time_record.get_timestamp(), str(datetime.utcnow()))
        self._log_message(WARNING, process_name, time_record, msg)

    def _process_state_processed(self, process_name, time_record):
        """method takes care of processing timetable records in STATE_PROCESSED state"""
        msg = 'Unexpected state %s of time-record %s' % (time_record.get_state(),
                                                        time_record.get_document()['_id'])
        self._log_message(ERROR, process_name, time_record, msg)

