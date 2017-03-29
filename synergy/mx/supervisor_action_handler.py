__author__ = 'Bohdan Mushkevych'

from werkzeug.utils import cached_property

from synergy.supervisor.supervisor_configurator import SupervisorConfigurator
from synergy.mx.base_request_handler import BaseRequestHandler, valid_action_request


# Supervisor Entries tab
class SupervisorActionHandler(BaseRequestHandler):
    def __init__(self, request, **values):
        super(SupervisorActionHandler, self).__init__(request, **values)

        self.box_id = self.request_arguments.get('box_id')
        self.process_name = self.request_arguments.get('process_name')
        self.is_request_valid = True if self.box_id and self.process_name else False

        if self.is_request_valid:
            self.box_id = self.box_id.strip()
            self.process_name = self.process_name.strip()
        self.configurator = SupervisorConfigurator(self.logger, self.box_id)

    @cached_property
    def entries(self):
        """ reading box configuration entries for all boxes managed by Synergy Supervisor """
        list_of_rows = []
        try:
            list_of_rows = self.configurator.bc_dao.get_all()
        except LookupError as e:
            self.logger.error('MX Exception {0}'.format(e), exc_info=True)
        return list_of_rows

    @valid_action_request
    def mark_for_start(self):
        self.configurator.mark_for_start(self.process_name)
        return self.reply_ok()

    @valid_action_request
    def mark_for_stop(self):
        self.configurator.mark_for_stop(self.process_name)
        return self.reply_ok()
