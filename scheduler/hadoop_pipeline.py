__author__ = 'Bohdan Mushkevych'

from db.error import DuplicateKeyError
from datetime import datetime
from logging import ERROR, WARNING, INFO

from abstract_pipeline import AbstractPipeline
from db.model.unit_of_work import UnitOfWork
from db.model import time_table_record, unit_of_work
from system import time_helper
from system.decorator import with_reconnect


class HadoopPipeline(AbstractPipeline):
    """ Pipeline to handle Hadoop mapreduce jobs """

    def __init__(self, logger, timetable):
        super(HadoopPipeline, self).__init__(logger, timetable)

    def __del__(self):
        super(HadoopPipeline, self).__del__()

    @with_reconnect
    def insert_uow(self, process_name, start_time, end_time, iteration, timetable_record):
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

    def _process_state_embryo(self, process_name, timetable_record, start_time):
        """ method that takes care of processing timetable records in STATE_EMBRYO state"""
        end_time = time_helper.increment_time(process_name, start_time)

        try:
            uow = self.insert_uow(process_name, start_time, end_time, 0, timetable_record)
        except DuplicateKeyError as e:
            uow = self.recover_from_duplicatekeyerror(e)
            msg = 'Catching up with latest unit_of_work %s in timeperiod %s, because of: %r' \
                  % (process_name, timetable_record.timeperiod, e)
            self._log_message(WARNING, process_name, timetable_record, msg)

        if uow is not None:
            self.timetable.update_timetable_record(process_name,
                                                   timetable_record,
                                                   uow,
                                                   time_table_record.STATE_IN_PROGRESS)
        else:
            msg = 'MANUAL INTERVENTION REQUIRED! Unable to locate unit_of_work for %s in %s' \
                  % (process_name, timetable_record.timeperiod)
            self._log_message(WARNING, process_name, timetable_record, msg)

    def _process_state_in_progress(self, process_name, timetable_record, start_time):
        """ method that takes care of processing timetable records in STATE_IN_PROGRESS state"""
        end_time = time_helper.increment_time(process_name, start_time)
        actual_time = time_helper.actual_time(process_name)
        can_finalize_timerecord = self.timetable.can_finalize_timetable_record(process_name, timetable_record)
        uow = self.uow_dao.get_one(timetable_record.related_unit_of_work)
        iteration = int(uow.end_id)

        try:
            if start_time == actual_time or can_finalize_timerecord is False:
                if uow.state in [unit_of_work.STATE_REQUESTED,
                                 unit_of_work.STATE_IN_PROGRESS,
                                 unit_of_work.STATE_INVALID]:
                    # Hadoop processing takes more than 1 tick of Scheduler
                    # Let the Hadoop processing complete - do no updates to Scheduler records
                    pass
                elif uow.state in [unit_of_work.STATE_PROCESSED,
                                   unit_of_work.STATE_CANCELED]:
                    # create new uow to cover new inserts
                    uow = self.insert_uow(process_name, start_time, end_time, iteration + 1, timetable_record)
                    self.timetable.update_timetable_record(process_name,
                                                           timetable_record,
                                                           uow,
                                                           time_table_record.STATE_IN_PROGRESS)

            elif start_time < actual_time and can_finalize_timerecord is True:
                if uow.state in [unit_of_work.STATE_REQUESTED,
                                 unit_of_work.STATE_IN_PROGRESS,
                                 unit_of_work.STATE_INVALID]:
                    # Hadoop processing has not started yet
                    # Let the Hadoop processing complete - do no updates to Scheduler records
                    pass
                elif uow.state in [unit_of_work.STATE_PROCESSED,
                                   unit_of_work.STATE_CANCELED]:
                    # create new uow for FINAL RUN
                    uow = self.insert_uow(process_name, start_time, end_time, iteration + 1, timetable_record)
                    self.timetable.update_timetable_record(process_name,
                                                           timetable_record,
                                                           uow,
                                                           time_table_record.STATE_FINAL_RUN)
            else:
                msg = 'Time-table-record %s has timeperiod from future %s vs current time %s' \
                      % (timetable_record.document['_id'], start_time, actual_time)
                self._log_message(ERROR, process_name, timetable_record, msg)

        except DuplicateKeyError as e:
            uow = self.recover_from_duplicatekeyerror(e)
            if uow is not None:
                self.timetable.update_timetable_record(process_name,
                                                       timetable_record,
                                                       uow,
                                                       timetable_record.state)
            else:
                msg = 'MANUAL INTERVENTION REQUIRED! Unable to identify unit_of_work for %s in %s' \
                      % (process_name, timetable_record.timeperiod)
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
