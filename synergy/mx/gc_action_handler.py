__author__ = 'Bohdan Mushkevych'

from synergy.mx.base_request_handler import BaseRequestHandler, valid_action_request


class GcActionHandler(BaseRequestHandler):
    def __init__(self, request, **values):
        super(GcActionHandler, self).__init__(request, **values)
        self.process_name = self.request_arguments.get('process_name')
        self.is_request_valid = True if self.process_name else False

        if self.is_request_valid:
            self.process_name = self.process_name.strip()

    def action_refresh(self):
        self.scheduler.gc.scan_uow_candidates()
        return self.reply_ok()

    def action_flush_all(self):
        self.scheduler.gc.flush(ignore_priority=True)
        return self.reply_ok()

    @valid_action_request
    def action_flush_one(self):
        self.scheduler.gc.flush_one(process_name=self.process_name, ignore_priority=True)
        return self.reply_ok()
