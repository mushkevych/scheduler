__author__ = 'Bohdan Mushkevych'

from logging import ERROR, INFO

from system import time_helper
from system.process_context import ProcessContext
from db.error import DuplicateKeyError
from db.model import time_table_record, unit_of_work
from scheduler.dicrete_pipeline import DiscretePipeline


class SimplifiedDiscretePipeline(DiscretePipeline):
    """ Pipeline to handle discrete timeperiod boundaries for jobs
    in comparison to DiscretePipeline this one does not transfer to STATE_FINAL_RUN"""

    def __init__(self, logger, timetable):
        super(SimplifiedDiscretePipeline, self).__init__(logger, timetable)

    def __del__(self):
        super(SimplifiedDiscretePipeline, self).__del__()

    def _process_state_in_progress(self, process_name, timetable_record, start_timeperiod):
        """ method that takes care of processing timetable records in STATE_IN_PROGRESS state"""
        time_qualifier = ProcessContext.get_time_qualifier(process_name)
        end_timeperiod = time_helper.increment_timeperiod(time_qualifier, start_timeperiod)
        actual_timeperiod = time_helper.actual_timeperiod(time_qualifier)
        can_finalize_timerecord = self.timetable.can_finalize_timetable_record(process_name, timetable_record)
        uow = self.uow_dao.get_one(timetable_record.related_unit_of_work)
        iteration = int(uow.end_id)

        try:
            if start_timeperiod == actual_timeperiod or can_finalize_timerecord is False:
                if uow.state in [unit_of_work.STATE_REQUESTED,
                                 unit_of_work.STATE_IN_PROGRESS,
                                 unit_of_work.STATE_INVALID]:
                    # Large Job processing takes more than 1 tick of Scheduler
                    # Let the Large Job processing complete - do no updates to Scheduler records
                    pass
                elif uow.state in [unit_of_work.STATE_PROCESSED,
                                   unit_of_work.STATE_CANCELED]:
                    # create new uow to cover new inserts
                    uow = self.insert_uow(process_name, start_timeperiod, end_timeperiod, iteration + 1, timetable_record)
                    self.timetable.update_timetable_record(process_name,
                                                           timetable_record,
                                                           uow,
                                                           time_table_record.STATE_IN_PROGRESS)

            elif start_timeperiod < actual_timeperiod and can_finalize_timerecord is True:
                if uow.state in [unit_of_work.STATE_REQUESTED,
                                 unit_of_work.STATE_IN_PROGRESS,
                                 unit_of_work.STATE_INVALID]:
                    # Job processing has not started yet
                    # Let the processing complete - do no updates to Scheduler records
                    msg = 'Suppressed creating uow for %s in timeperiod %s; timetable_record is in %s; uow is in %s' \
                          % (process_name, timetable_record.timeperiod, timetable_record.state, uow.state)
                elif uow.state == unit_of_work.STATE_PROCESSED:
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
                    msg = 'Unknown state %s for time-table-record %s in timeperiod %s for %s' \
                          % (uow.state, timetable_record.document['_id'], timetable_record.timeperiod, process_name)

                self._log_message(INFO, process_name, timetable_record, msg)
            else:
                msg = 'Time-table-record %s has timeperiod from future %s vs current time %s' \
                      % (timetable_record.document['_id'], start_timeperiod, actual_timeperiod)
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
        raise NotImplementedError('Method _process_state_final_run is not supported by SimplifiedDiscretePipeline')
