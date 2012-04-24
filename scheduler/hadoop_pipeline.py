"""
Created on 2011-02-07

@author: Bohdan Mushkevych
"""

from pymongo.errors import DuplicateKeyError
from pymongo.objectid import ObjectId
from datetime import datetime
from logging import ERROR, WARNING, INFO

from abstract_pipeline import AbstractPipeline
from model import unit_of_work_helper
from model.unit_of_work_entry import UnitOfWorkEntry
from model.time_table_entry import TimeTableEntry
from system import time_helper
from system.decorator import with_reconnect


class HadoopPipeline(AbstractPipeline):
    """ Scheduler encapsulate logic for starting the aggregators/alerts/googlereport reader """

    def __init__(self, scheduler, timetable):
        super(HadoopPipeline, self).__init__(scheduler, timetable)


    def __del__(self):
        super(HadoopPipeline, self).__del__()


    @with_reconnect
    def insert_uow(self, process_name, start_time, end_time, iteration, time_record):
        """ creates unit_of_work and inserts it into the MongoDB
            @raise DuplicateKeyError if unit_of_work with given parameters already exists """
        first_object_id = 0
        last_object_id = iteration

        unit_of_work = UnitOfWorkEntry()
        unit_of_work.set_timestamp(start_time)
        unit_of_work.set_start_id(first_object_id)
        unit_of_work.set_end_id(last_object_id)
        unit_of_work.set_start_timestamp(start_time)
        unit_of_work.set_end_timestamp(end_time)
        unit_of_work.set_created_at(datetime.utcnow())
        unit_of_work.set_source_collection(None)
        unit_of_work.set_target_collection(None)
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


    def _process_state_embryo(self, process_name, time_record, start_time):
        """ method that takes care of processing timetable records in STATE_EMBRYO state"""
        end_time = time_helper.increment_time(process_name, start_time)

        uow_obj = None
        try:
            uow_obj = self.insert_uow(process_name, start_time, end_time, 0, time_record)
        except DuplicateKeyError as e:
            uow_obj = self.recover_from_duplicatekeyerror(e)
            msg = 'Catching up with latest unit_of_work %s in timeperiod %s, because of: %r'\
            % (process_name, time_record.get_timestamp(), e)
            self._log_message(WARNING, process_name, time_record, msg)

        if uow_obj is not None:
            self.timetable.update_timetable_record(process_name,
                                                   time_record,
                                                   uow_obj,
                                                   TimeTableEntry.STATE_IN_PROGRESS)
        else:
            msg = 'MANUAL INTERVENTION REQUIRED! Unable to locate unit_of_work for %s in %s'\
            % (process_name, time_record.get_timestamp())
            self._log_message(WARNING, process_name, time_record, msg)


    def _process_state_in_progress(self, process_name, time_record, start_time):
        """ method that takes care of processing timetable records in STATE_IN_PROGRESS state"""
        end_time = time_helper.increment_time(process_name, start_time)
        actual_time = time_helper.actual_time(process_name)
        can_finalize_timerecord = self.timetable.can_finalize_timetable_record(process_name, time_record)
        uow_id = time_record.get_related_unit_of_work()
        uow_obj = unit_of_work_helper.retrieve_by_id(self.logger, ObjectId(uow_id))
        iteration = int(uow_obj.get_end_id())

        try:
            if start_time == actual_time or can_finalize_timerecord == False:
                if uow_obj.get_state() == UnitOfWorkEntry.STATE_REQUESTED\
                    or uow_obj.get_state() == UnitOfWorkEntry.STATE_IN_PROGRESS\
                    or uow_obj.get_state() == UnitOfWorkEntry.STATE_INVALID:
                    # Hadoop processing takes more than 1 tick of Scheduler
                    # Let the Hadoop processing complete - do no updates to Scheduler records
                    pass
                elif uow_obj.get_state() == UnitOfWorkEntry.STATE_PROCESSED\
                    or uow_obj.get_state() == UnitOfWorkEntry.STATE_CANCELED:
                    # create new uow to cover new inserts
                    uow_obj = self.insert_uow(process_name, start_time, end_time, iteration + 1, time_record)
                    self.timetable.update_timetable_record(process_name,
                                                           time_record,
                                                           uow_obj,
                                                           TimeTableEntry.STATE_IN_PROGRESS)

            elif start_time < actual_time and can_finalize_timerecord == True:
                if uow_obj.get_state() == UnitOfWorkEntry.STATE_REQUESTED\
                    or uow_obj.get_state() == UnitOfWorkEntry.STATE_IN_PROGRESS\
                    or uow_obj.get_state() == UnitOfWorkEntry.STATE_INVALID:
                    # Hadoop processing has not started yet
                    # Let the Hadoop processing complete - do no updates to Scheduler records
                    pass
                elif uow_obj.get_state() == UnitOfWorkEntry.STATE_PROCESSED\
                    or uow_obj.get_state() == UnitOfWorkEntry.STATE_CANCELED:
                    # create new uow for FINAL RUN
                    uow_obj = self.insert_uow(process_name, start_time, end_time, iteration + 1, time_record)
                    self.timetable.update_timetable_record(process_name,
                                                           time_record,
                                                           uow_obj,
                                                           TimeTableEntry.STATE_FINAL_RUN)
            else:
                msg = 'Time-record %s has timestamp from future %s vs current time %s'\
                        % (time_record.get_document()['_id'], start_time, actual_time)
                self._log_message(ERROR, process_name, time_record, msg)

        except DuplicateKeyError as e:
            uow_obj = self.recover_from_duplicatekeyerror(e)
            if uow_obj is not None:
                self.timetable.update_timetable_record(process_name,
                                                       time_record,
                                                       uow_obj,
                                                       time_record.get_state())
            else:
                msg = 'MANUAL INTERVENTION REQUIRED! Unable to identify unit_of_work for %s in %s'\
                % (process_name, time_record.get_timestamp())
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
        msg = 'Skipping time-record %s in timeperiod %s. Apparently its most current timeperiod as of %s UTC'\
        % (time_record.get_document()['_id'], time_record.get_timestamp(), str(datetime.utcnow()))
        self._log_message(WARNING, process_name, time_record, msg)


    def _process_state_processed(self, process_name, time_record):
        """method takes care of processing timetable records in STATE_PROCESSED state"""
        msg = 'Unexpected state %s of time-record %s' % (time_record.get_state(),
                                                         time_record.get_document()['_id'])
        self._log_message(ERROR, process_name, time_record, msg)

