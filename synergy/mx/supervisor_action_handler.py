__author__ = 'Bohdan Mushkevych'

import functools
from werkzeug.utils import cached_property

from synergy.db.dao.box_configuration_dao import BoxConfigurationDao
from synergy.supervisor.supervisor_configurator import SupervisorConfigurator
from synergy.mx.base_request_handler import BaseRequestHandler, valid_action_request


def valid_configurator(method):
    @functools.wraps(method)
    def _wrapper(self, *args, **kwargs):
        assert isinstance(self, SupervisorActionHandler)
        try:
            self.configurator
        except (LookupError, EnvironmentError) as e:
            return self.reply_server_error(e)
        return method(self, *args, **kwargs)
    return _wrapper


# Supervisor Entries tab
class SupervisorActionHandler(BaseRequestHandler):
    def __init__(self, request, **values):
        super(SupervisorActionHandler, self).__init__(request, **values)
        self.bc_dao = BoxConfigurationDao(self.logger)

        self.box_id = self.request_arguments.get('box_id')
        self.process_name = self.request_arguments.get('process_name')
        self.is_request_valid = True if self.box_id and self.process_name else False

        if self.is_request_valid:
            self.box_id = self.box_id.strip()
            self.process_name = self.process_name.strip()
        self._configurator = None

    @property
    def configurator(self):
        if not self._configurator:
            self._configurator = SupervisorConfigurator(self.logger, self.box_id)
        return self._configurator

    @cached_property
    def entries(self):
        """ reading box configuration entries for all boxes managed by Synergy Supervisor """
        list_of_rows = []
        try:
            list_of_rows = self.bc_dao.get_all()
        except LookupError as e:
            self.logger.error(f'MX Exception {e}', exc_info=True)
        return list_of_rows

    @valid_configurator
    @valid_action_request
    def mark_for_start(self):
        self.configurator.mark_for_start(self.process_name)
        return self.reply_ok()

    @valid_configurator
    @valid_action_request
    def mark_for_stop(self):
        self.configurator.mark_for_stop(self.process_name)
        return self.reply_ok()
