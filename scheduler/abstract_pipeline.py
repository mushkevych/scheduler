__author__ = 'Bohdan Mushkevych'

from datetime import datetime
from logging import INFO, WARNING, ERROR

from flopsy.flopsy import PublishersPool
from model import unit_of_work_helper
from model import time_table
from system.decorator import with_reconnect


class AbstractPipeline(object):
    """ Scheduler encapsulate logic for initiating worker processes """

    def __init__(self, scheduler, timetable):
        self.logger = scheduler.logger
        self.publishers = PublishersPool(self.logger)
        self.scheduler = scheduler
        self.timetable = timetable

    def __del__(self):
        pass

    def _log_message(self, level, process_name, time_record, msg):
        """ method performs logging into log file and TimeTable node"""
        self.timetable.add_log_entry(process_name, time_record, datetime.utcnow(), msg)
        self.logger.log(level, msg)

    @with_reconnect
    def recover_from_duplicatekeyerror(self, e):
        """ try to recover from DuplicateKeyError """
        first_object_id = None
        last_object_id = None
        process_name = None
        timestamp = None

        if hasattr(e, 'first_object_id'):
            first_object_id = e.first_object_id
        if hasattr(e, 'last_object_id'):
            last_object_id = e.last_object_id
        if hasattr(e, 'process_name'):
            process_name = e.process_name
        if hasattr(e, 'timestamp'):
            timestamp = e.timestamp

        if first_object_id is not None \
                and last_object_id is not None \
                and process_name is not None \
                and timestamp is not None:
            try:
                return unit_of_work_helper.retrieve_by_params(self.logger,
                                                              process_name,
                                                              timestamp,
                                                              first_object_id,
                                                              last_object_id)
            except LookupError:
                pass

    def _process_state_embryo(self, process_name, time_record, start_time):
        """ method that takes care of processing timetable records in STATE_EMBRYO state"""
        pass

    def _process_state_in_progress(self, process_name, time_record, start_time):
        """ method that takes care of processing timetable records in STATE_IN_PROGRESS state"""
        pass

    def _process_state_final_run(self, process_name, time_record):
        """method takes care of processing timetable records in STATE_FINAL_RUN state"""
        pass

    def _process_state_skipped(self, process_name, time_record):
        """method takes care of processing timetable records in STATE_FINAL_SKIPPED state"""
        pass

    def _process_state_processed(self, process_name, time_record):
        """method takes care of processing timetable records in STATE_FINAL_SKIPPED state"""
        pass

    def manage_pipeline_with_blocking_dependencies(self, process_name, time_record):
        """ method holds logic for triggering/holding processing of time_period if its dependencies are not processed"""
        green_light, skipped_present = self.timetable.is_dependent_on_finalized(process_name, time_record)
        if green_light:
            self.manage_pipeline_for_process(process_name, time_record)
        elif skipped_present:
            # As soon as among <dependent on> periods are in STATE_SKIPPED
            # there is very little sense in waiting for them to become STATE_PROCESSED
            # Skip this timeperiod itself
            time_record.set_state(time_table.STATE_SKIPPED)
            self.timetable._save_time_record(process_name, time_record)
            tree = self.timetable.get_tree(process_name)
            tree.update_node_by_process(process_name, time_record)

            msg = '%s for timeperiod %r is blocked by STATE_SKIPPED dependencies. Transferred timeperiod itself to STATE_SKIPPED' \
                  % (process_name, time_record.timeperiod)
            self._log_message(WARNING, process_name, time_record, msg)
        else:
            msg = '%s for timeperiod %r is blocked by unprocessed dependencies. Waiting another tick' \
                  % (process_name, time_record.timeperiod)
            self._log_message(INFO, process_name, time_record, msg)


    def manage_pipeline_for_process(self, process_name, time_record):
        """ method main duty - do _not_ publish another unit_of_work, if previous was not yet processed
        In case Scheduler sees that unit_of_work is pending - it just updates boundaries of the processing"""
        timestamp_tr = time_record.timeperiod
        try:
            if time_record.state == time_table.STATE_EMBRYO:
                self._process_state_embryo(process_name, time_record, timestamp_tr)

            elif time_record.state == time_table.STATE_IN_PROGRESS:
                self._process_state_in_progress(process_name, time_record, timestamp_tr)

            elif time_record.state == time_table.STATE_FINAL_RUN:
                self._process_state_final_run(process_name, time_record)

            elif time_record.state == time_table.STATE_SKIPPED:
                self._process_state_skipped(process_name, time_record)

            elif time_record.state == time_table.STATE_PROCESSED:
                self._process_state_processed(process_name, time_record)

            else:
                msg = 'Unknown state %s of time-record %s' % (time_record._state,
                                                              time_record.document['_id'])
                self._log_message(ERROR, process_name, time_record, msg)

        except LookupError as e:
            self.timetable.failed_on_processing_timetable_record(process_name, timestamp_tr)
            msg = 'Increasing fail counter for %s in timeperiod %s, because of: %r' % (process_name, timestamp_tr, e)
            self._log_message(WARNING, process_name, time_record, msg)
