__author__ = 'Bohdan Mushkevych'

from werkzeug.utils import cached_property

from synergy.mx.rest_model_factory import create_rest_managed_scheduler_entry, create_rest_freerun_scheduler_entry
from synergy.system.performance_tracker import FootprintCalculator


# Scheduler Entries Details tab
class SchedulerEntries(object):
    def __init__(self, mbean):
        self.mbean = mbean
        self.logger = self.mbean.logger

    @cached_property
    def managed_entries(self):
        list_of_rows = []
        try:
            sorter_keys = sorted(self.mbean.managed_handlers.keys())
            for key in sorter_keys:
                thread_handler = self.mbean.managed_handlers[key]
                rest_model = create_rest_managed_scheduler_entry(thread_handler, self.mbean.timetable)
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
                rest_model = create_rest_freerun_scheduler_entry(thread_handler)
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
