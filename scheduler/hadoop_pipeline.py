__author__ = 'Bohdan Mushkevych'


from bson.objectid import ObjectId
from pymongo.errors import DuplicateKeyError
from datetime import datetime
from logging import ERROR, WARNING, INFO

from abstract_pipeline import AbstractPipeline
from model import unit_of_work_dao, unit_of_work
from model.unit_of_work import UnitOfWork
from model import time_table
from system import time_helper
from system.decorator import with_reconnect


class HadoopPipeline(AbstractPipeline):
    """ Pipeline to handle Hadoop mapreduce jobs """

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

        uow = UnitOfWork()
        uow.timeperiod = start_time
        uow.start_id = first_object_id
        uow.end_id = last_object_id
        uow.start_timeperiod = start_time
        uow.end_timeperiod = end_time
        uow.created_at = datetime.utcnow()
        uow.source_collection = None
        uow.target_collection = None
        uow.state = unit_of_work.STATE_REQUESTED
        uow.process_name = process_name
        uow.number_of_retries = 0

        try:
            uow_id = unit_of_work_dao.insert(self.logger, uow)
        except DuplicateKeyError as e:
            e.first_object_id = str(first_object_id)
            e.last_object_id = str(last_object_id)
            e.process_name = process_name
            e.timeperiod = start_time
            raise e

        self.publishers.get_publisher(process_name).publish(str(uow_id))
        msg = 'Published: UOW %r for %r in timeperiod %r.' % (uow_id, process_name, start_time)
        self._log_message(INFO, process_name, time_record, msg)
        return uow

    def _process_state_embryo(self, process_name, time_record, start_time):
        """ method that takes care of processing timetable records in STATE_EMBRYO state"""
        end_time = time_helper.increment_time(process_name, start_time)

        uow_obj = None
        try:
            uow_obj = self.insert_uow(process_name, start_time, end_time, 0, time_record)
        except DuplicateKeyError as e:
            uow_obj = self.recover_from_duplicatekeyerror(e)
            msg = 'Catching up with latest unit_of_work %s in timeperiod %s, because of: %r' \
                  % (process_name, time_record.timeperiod, e)
            self._log_message(WARNING, process_name, time_record, msg)

        if uow_obj is not None:
            self.timetable.update_timetable_record(process_name,
                                                   time_record,
                                                   uow_obj,
                                                   time_table.STATE_IN_PROGRESS)
        else:
            msg = 'MANUAL INTERVENTION REQUIRED! Unable to locate unit_of_work for %s in %s' \
                  % (process_name, time_record.timeperiod)
            self._log_message(WARNING, process_name, time_record, msg)

    def _process_state_in_progress(self, process_name, time_record, start_time):
        """ method that takes care of processing timetable records in STATE_IN_PROGRESS state"""
        end_time = time_helper.increment_time(process_name, start_time)
        actual_time = time_helper.actual_time(process_name)
        can_finalize_timerecord = self.timetable.can_finalize_timetable_record(process_name, time_record)
        uow_id = time_record.related_unit_of_work
        uow_obj = unit_of_work_dao.retrieve_by_id(self.logger, ObjectId(uow_id))
        iteration = int(uow_obj.end_id)

        try:
            if start_time == actual_time or can_finalize_timerecord is False:
                if uow_obj.state in [unit_of_work.STATE_REQUESTED,
                                       unit_of_work.STATE_IN_PROGRESS,
                                       unit_of_work.STATE_INVALID]:
                    # Hadoop processing takes more than 1 tick of Scheduler
                    # Let the Hadoop processing complete - do no updates to Scheduler records
                    pass
                elif uow_obj.state in [unit_of_work.STATE_PROCESSED,
                                       unit_of_work.STATE_CANCELED]:
                    # create new uow to cover new inserts
                    uow_obj = self.insert_uow(process_name, start_time, end_time, iteration + 1, time_record)
                    self.timetable.update_timetable_record(process_name,
                                                           time_record,
                                                           uow_obj,
                                                           time_table.STATE_IN_PROGRESS)

            elif start_time < actual_time and can_finalize_timerecord is True:
                if uow_obj.state in [unit_of_work.STATE_REQUESTED,
                                     unit_of_work.STATE_IN_PROGRESS,
                                     unit_of_work.STATE_INVALID]:
                    # Hadoop processing has not started yet
                    # Let the Hadoop processing complete - do no updates to Scheduler records
                    pass
                elif uow_obj.state in [unit_of_work.STATE_PROCESSED,
                                       unit_of_work.STATE_CANCELED]:
                    # create new uow for FINAL RUN
                    uow_obj = self.insert_uow(process_name, start_time, end_time, iteration + 1, time_record)
                    self.timetable.update_timetable_record(process_name,
                                                           time_record,
                                                           uow_obj,
                                                           time_table.STATE_FINAL_RUN)
            else:
                msg = 'Time-record %s has timeperiod from future %s vs current time %s' \
                      % (time_record.document['_id'], start_time, actual_time)
                self._log_message(ERROR, process_name, time_record, msg)

        except DuplicateKeyError as e:
            uow_obj = self.recover_from_duplicatekeyerror(e)
            if uow_obj is not None:
                self.timetable.update_timetable_record(process_name,
                                                       time_record,
                                                       uow_obj,
                                                       time_record.state)
            else:
                msg = 'MANUAL INTERVENTION REQUIRED! Unable to identify unit_of_work for %s in %s' \
                      % (process_name, time_record.timeperiod)
                self._log_message(ERROR, process_name, time_record, msg)

    def _process_state_final_run(self, process_name, time_record):
        """method takes care of processing timetable records in STATE_FINAL_RUN state"""
        uow_id = time_record.related_unit_of_work
        uow_obj = unit_of_work_dao.retrieve_by_id(self.logger, ObjectId(uow_id))

        if uow_obj.state == unit_of_work.STATE_PROCESSED:
            self.timetable.update_timetable_record(process_name,
                                                   time_record,
                                                   uow_obj,
                                                   time_table.STATE_PROCESSED)
            timetable_tree = self.timetable.get_tree(process_name)
            timetable_tree.build_tree()
            msg = 'Transferred time-record %s in timeperiod %s to STATE_PROCESSED for %s' \
                  % (time_record.document['_id'], time_record.timeperiod, process_name)
        elif uow_obj.state == unit_of_work.STATE_CANCELED:
            self.timetable.update_timetable_record(process_name,
                                                   time_record,
                                                   uow_obj,
                                                   time_table.STATE_SKIPPED)
            msg = 'Transferred time-record %s in timeperiod %s to STATE_SKIPPED for %s' \
                  % (time_record.document['_id'], time_record.timeperiod, process_name)
        else:
            msg = 'Suppressed creating uow for %s in timeperiod %s; time_record is in %s; uow is in %s' \
                  % (process_name, time_record.timeperiod, time_record.state, uow_obj.state)
        self._log_message(INFO, process_name, time_record, msg)

    def _process_state_skipped(self, process_name, time_record):
        """method takes care of processing timetable records in STATE_SKIPPED state"""
        msg = 'Skipping time-record %s in timeperiod %s. Apparently its most current timeperiod as of %s UTC' \
              % (time_record.document['_id'], time_record.timeperiod, str(datetime.utcnow()))
        self._log_message(WARNING, process_name, time_record, msg)

    def _process_state_processed(self, process_name, time_record):
        """method takes care of processing timetable records in STATE_PROCESSED state"""
        msg = 'Unexpected state %s of time-record %s' % (time_record.state,
                                                         time_record.document['_id'])
        self._log_message(ERROR, process_name, time_record, msg)
