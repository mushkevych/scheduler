__author__ = 'Bohdan Mushkevych'

from werkzeug.utils import cached_property

from synergy.system.event_clock import format_time_trigger_string
from synergy.system.performance_tracker import FootprintCalculator
from synergy.mx.rest_models import RestFreerunSchedulerEntry, RestManagedSchedulerEntry


# Scheduler Entries Details tab
class SchedulerEntries(object):
    def __init__(self, mbean):
        self.mbean = mbean
        self.logger = self.mbean.logger

    def _handler_next_run(self, thread_handler):
        if not thread_handler.is_alive:
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
                process_name = thread_handler.key

                rest_model = RestManagedSchedulerEntry(
                    is_on=thread_handler.process_entry.is_on,
                    is_alive=thread_handler.is_alive,
                    process_name=process_name,
                    trigger_frequency=format_time_trigger_string(thread_handler.timer_instance),
                    next_run_in=self._handler_next_run(thread_handler),
                    next_timeperiod=self._handler_next_timeperiod(process_name)
                )

                list_of_rows.append(rest_model.document)
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
                process_name, entry_name = thread_handler.key

                rest_model = RestFreerunSchedulerEntry(
                    is_on=thread_handler.process_entry.is_on,
                    is_alive=thread_handler.is_alive,
                    process_name=process_name,
                    entry_name=entry_name,
                    trigger_frequency=format_time_trigger_string(thread_handler.timer_instance),
                    next_run_in=self._handler_next_run(thread_handler),
                    arguments=thread_handler.process_entry.arguments
                )

                list_of_rows.append(rest_model.document)
        except Exception as e:
            self.logger.error('MX Exception %s' % str(e), exc_info=True)

        return list_of_rows

    @cached_property
    def footprint(self):
        try:
            calculator = FootprintCalculator()
            return calculator.document
        except Exception as e:
            self.logger.error('MX Exception %s' % str(e), exc_info=True)
            return []
