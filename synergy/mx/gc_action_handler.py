__author__ = 'Bohdan Mushkevych'

from synergy.system.utils import tail_file
from synergy.system.system_logger import get_log_filename
from synergy.mx.base_request_handler import BaseRequestHandler, valid_action_request
from synergy.scheduler.scheduler_constants import PROCESS_GC


class GcActionHandler(BaseRequestHandler):
    """ Garbage Collector UI action handler """

    def __init__(self, request, **values):
        super(GcActionHandler, self).__init__(request, **values)
        self.process_name = self.request_arguments.get('process_name')
        self.is_request_valid = True if self.process_name else False

        if self.is_request_valid:
            self.process_name = self.process_name.strip()

    def refresh(self):
        self.scheduler.gc.validate()
        self.scheduler.gc.scan_uow_candidates()
        self.logger.info('MX: performed GC Refresh')
        return self.reply_ok()

    def flush_all(self):
        self.scheduler.gc.flush(ignore_priority=True)
        self.logger.info('MX: performed GC Flush All')
        return self.reply_ok()

    @valid_action_request
    def flush_one(self):
        self.scheduler.gc.flush_one(process_name=self.process_name, ignore_priority=True)
        self.logger.info(f'MX: performed GC Flush for {self.process_name}')
        return self.reply_ok()

    def tail_gc_log(self):
        fqfn = get_log_filename(PROCESS_GC)
        return tail_file(fqfn)
