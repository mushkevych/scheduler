__author__ = 'Bohdan Mushkevych'

from werkzeug.utils import cached_property

from synergy.db.model import scheduler_managed_entry
from synergy.system.event_clock import format_time_trigger_string
from synergy.system.performance_tracker import FootprintCalculator


# Scheduler Entries Details tab
class SchedulerEntries(object):
    def __init__(self, mbean):
        self.mbean = mbean
        self.logger = self.mbean.logger

    def _handler_next_run(self, thread_handler):
        if not thread_handler.is_alive():
            return 'NA'

        next_run = thread_handler.next_run_in()
        return str(next_run).split('.')[0]

    def _handler_next_timeperiod(self, process_name):
        timetable = self.mbean.timetable
        if timetable.get_tree(process_name) is None:
            return 'NA'
        else:
            job_record = timetable.get_next_job_record(process_name)
            return job_record.timeperiod

    @cached_property
    def managed_entries(self):
        list_of_rows = []
        try:
            sorter_keys = sorted(self.mbean.managed_handlers.keys())
            for key in sorter_keys:
                thread_handler = self.mbean.managed_handlers[key]
                process_name = thread_handler.arguments.key

                row = []
                # indicate whether process is in active or passive state
                # parameters are set in Scheduler.run() method
                is_on = thread_handler.arguments.scheduler_entry_obj.state == scheduler_managed_entry.STATE_ON
                row.append(is_on)                                                               # index 0
                row.append(thread_handler.is_alive())                                           # index 1
                row.append(process_name)                                                        # index 2
                row.append(format_time_trigger_string(thread_handler.timer_instance))           # index 3
                row.append(self._handler_next_run(thread_handler))                              # index 4
                row.append(self._handler_next_timeperiod(process_name))                         # index 5

                list_of_rows.append(row)
        except Exception as e:
            self.logger.error('MX Exception %s' % str(e), exc_info=True)

        return list_of_rows

    @cached_property
    def freerun_entries(self):
        list_of_rows = []
        try:
            sorter_keys = sorted(self.mbean.freerun_handlers.keys())
            for key in sorter_keys:
                thread_handler = self.mbean.freerun_handlers[key]
                process_name, entry_name = thread_handler.arguments.key

                row = []
                # indicate whether process is in active or passive state
                # parameters are set in Scheduler.run() method
                is_on = thread_handler.arguments.scheduler_entry_obj.state == scheduler_managed_entry.STATE_ON
                row.append(is_on)                                                               # index 0
                row.append(thread_handler.is_alive())                                           # index 1
                row.append(process_name)                                                        # index 2
                row.append(entry_name)                                                          # index 3
                row.append(format_time_trigger_string(thread_handler.timer_instance))           # index 4
                row.append(self._handler_next_run(thread_handler))                              # index 5
                row.append(thread_handler.arguments.scheduler_entry_obj.arguments)              # index 6

                list_of_rows.append(row)
        except Exception as e:
            self.logger.error('MX Exception %s' % str(e), exc_info=True)

        return list_of_rows

    @cached_property
    def footprint(self):
        try:
            calculator = FootprintCalculator()
            footprint = calculator.get_snapshot_as_list()
            return footprint
        except Exception as e:
            self.logger.error('MX Exception %s' % str(e), exc_info=True)
