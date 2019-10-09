__author__ = 'Bohdan Mushkevych'

from werkzeug.utils import cached_property

from synergy.scheduler.scheduler_constants import PROCESS_SCHEDULER, PROCESS_MX
from synergy.mx.base_request_handler import BaseRequestHandler
from synergy.mx.rest_model_factory import create_rest_managed_scheduler_entry, create_rest_freerun_scheduler_entry
from synergy.system.performance_tracker import FootprintCalculator
from synergy.system.system_logger import get_log_filename
from synergy.system.utils import tail_file


# Scheduler Entries Details tab
class SchedulerEntries(BaseRequestHandler):
    def __init__(self, request, **values):
        super(SchedulerEntries, self).__init__(request, **values)

    @cached_property
    def managed_entries(self):
        list_of_rows = []
        try:
            handler_keys = list(self.scheduler.managed_handlers)
            for key in sorted(handler_keys):
                thread_handler = self.scheduler.managed_handlers[key]
                rest_model = create_rest_managed_scheduler_entry(thread_handler,
                                                                 self.scheduler.timetable,
                                                                 self.scheduler.gc)
                list_of_rows.append(rest_model.document)
        except Exception as e:
            self.logger.error(f'MX Exception {e}', exc_info=True)

        return list_of_rows

    @cached_property
    def freerun_entries(self):
        list_of_rows = []
        try:
            handler_keys = list(self.scheduler.freerun_handlers)
            for key in sorted(handler_keys):
                thread_handler = self.scheduler.freerun_handlers[key]
                rest_model = create_rest_freerun_scheduler_entry(thread_handler)
                list_of_rows.append(rest_model.document)
        except Exception as e:
            self.logger.error(f'MX Exception {e}', exc_info=True)

        return list_of_rows

    @cached_property
    def footprint(self):
        try:
            calculator = FootprintCalculator()
            return calculator.document
        except Exception as e:
            self.logger.error(f'MX Exception {e}', exc_info=True)
            return []

    @cached_property
    def reprocess_uows(self):
        return self.scheduler.gc.reprocess_uows

    def tail_scheduler_log(self):
        fqfn = get_log_filename(PROCESS_SCHEDULER)
        return tail_file(fqfn)

    def tail_mx_log(self):
        fqfn = get_log_filename(PROCESS_MX)
        return tail_file(fqfn)
