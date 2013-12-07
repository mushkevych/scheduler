__author__ = 'Bohdan Mushkevych'

from db.model import time_table_record
from db.dao.unit_of_work_dao import UnitOfWorkDao
from db.dao.time_table_record_dao import TimeTableRecordDao
from datetime import datetime
from logging import INFO, WARNING, ERROR

from flopsy.flopsy import PublishersPool
from system.decorator import with_reconnect


class AbstractPipeline(object):
    """ Scheduler encapsulate logic for initiating worker processes """

    def __init__(self, logger, timetable):
        self.logger = logger
        self.publishers = PublishersPool(self.logger)
        self.timetable = timetable
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.ttr_dao = TimeTableRecordDao(self.logger)

    def __del__(self):
        pass

    def _log_message(self, level, process_name, timetable_record, msg):
        """ method performs logging into log file and TimeTable node"""
        self.timetable.add_log_entry(process_name, timetable_record, datetime.utcnow(), msg)
        self.logger.log(level, msg)

    @with_reconnect
    def recover_from_duplicatekeyerror(self, e):
        """ try to recover from DuplicateKeyError """
        first_object_id = None
        last_object_id = None
        process_name = None
        timeperiod = None

        if hasattr(e, 'first_object_id'):
            first_object_id = e.first_object_id
        if hasattr(e, 'last_object_id'):
            last_object_id = e.last_object_id
        if hasattr(e, 'process_name'):
            process_name = e.process_name
        if hasattr(e, 'timeperiod'):
            timeperiod = e.timeperiod

        if first_object_id is not None \
            and last_object_id is not None \
            and process_name is not None \
            and timeperiod is not None:
            try:
                return self.uow_dao.get_by_params(process_name,
                                                  timeperiod,
                                                  first_object_id,
                                                  last_object_id)
            except LookupError:
                pass

    def _process_state_embryo(self, process_name, timetable_record, start_timeperiod):
        """ method that takes care of processing timetable records in STATE_EMBRYO state"""
        pass

    def _process_state_in_progress(self, process_name, timetable_record, start_timeperiod):
        """ method that takes care of processing timetable records in STATE_IN_PROGRESS state"""
        pass

    def _process_state_final_run(self, process_name, timetable_record):
        """method takes care of processing timetable records in STATE_FINAL_RUN state"""
        pass

    def _process_state_skipped(self, process_name, timetable_record):
        """method takes care of processing timetable records in STATE_FINAL_SKIPPED state"""
        pass

    def _process_state_processed(self, process_name, timetable_record):
        """method takes care of processing timetable records in STATE_FINAL_SKIPPED state"""
        pass

    def manage_pipeline_with_blocking_dependencies(self, process_name, timetable_record):
        """ method holds logic for triggering/holding processing
        of timetable_record if its dependencies are not processed"""
        green_light, skipped_present = self.timetable.is_dependent_on_finalized(process_name, timetable_record)
        if green_light:
            self.manage_pipeline_for_process(process_name, timetable_record)
        elif skipped_present:
            # As soon as among <dependent on> periods are in STATE_SKIPPED
            # there is very little sense in waiting for them to become STATE_PROCESSED
            # Skip this timeperiod itself
            timetable_record.state = time_table_record.STATE_SKIPPED
            self.ttr_dao.update(timetable_record)
            tree = self.timetable.get_tree(process_name)
            tree.update_node_by_process(process_name, timetable_record)

            msg = '%s for timeperiod %r is blocked by STATE_SKIPPED dependencies. Transferred timeperiod itself to STATE_SKIPPED' \
                  % (process_name, timetable_record.timeperiod)
            self._log_message(WARNING, process_name, timetable_record, msg)
        else:
            msg = '%s for timeperiod %r is blocked by unprocessed dependencies. Waiting another tick' \
                  % (process_name, timetable_record.timeperiod)
            self._log_message(INFO, process_name, timetable_record, msg)

    def manage_pipeline_for_process(self, process_name, timetable_record):
        """ method main duty - do _not_ publish another unit_of_work, if previous was not yet processed
        In case Scheduler sees that unit_of_work is pending - it just updates boundaries of the processing"""
        timeperiod_tr = timetable_record.timeperiod
        try:
            if timetable_record.state == time_table_record.STATE_EMBRYO:
                self._process_state_embryo(process_name, timetable_record, timeperiod_tr)

            elif timetable_record.state == time_table_record.STATE_IN_PROGRESS:
                self._process_state_in_progress(process_name, timetable_record, timeperiod_tr)

            elif timetable_record.state == time_table_record.STATE_FINAL_RUN:
                self._process_state_final_run(process_name, timetable_record)

            elif timetable_record.state == time_table_record.STATE_SKIPPED:
                self._process_state_skipped(process_name, timetable_record)

            elif timetable_record.state == time_table_record.STATE_PROCESSED:
                self._process_state_processed(process_name, timetable_record)

            else:
                msg = 'Unknown state %s of time-table-record %s' % (timetable_record._state,
                                                                    timetable_record.document['_id'])
                self._log_message(ERROR, process_name, timetable_record, msg)

        except LookupError as e:
            self.timetable.failed_on_processing_timetable_record(process_name, timeperiod_tr)
            msg = 'Increasing fail counter for %s in timeperiod %s, because of: %r' % (process_name, timeperiod_tr, e)
            self._log_message(WARNING, process_name, timetable_record, msg)
