__author__ = 'Bohdan Mushkevych'

from werkzeug.utils import cached_property

from synergy.db.dao.box_configuration_dao import BoxConfigurationDao
from synergy.mx.base_request_handler import BaseRequestHandler, valid_action_request


# Supervisor Entries tab
class SupervisorActionHandler(BaseRequestHandler):
    def __init__(self, request, **values):
        super(SupervisorActionHandler, self).__init__(request, **values)
        self.bc_dao = BoxConfigurationDao(self.logger)

        self.box_id = self.request_arguments.get('box_id')
        self.process_name = self.request_arguments.get('process_name')
        self.is_request_valid = True if self.box_id and self.process_name else False

        if self.is_request_valid:
            self.process_name = self.process_name.strip()

    @cached_property
    def entries(self):
        """ reading box configuration entries """
        list_of_rows = []
        try:
            list_of_rows = self.bc_dao.get_all()
        except LookupError as e:
            self.logger.error('MX Exception {0}'.format(e), exc_info=True)
        return list_of_rows

    def _change_state(self, new_state):
        self.logger.info('MX: requesting supervisor to set state {0} for process {1} on box {2} \n'
                         .format(new_state, self.process_name, self.box_id))
        box_config = self.bc_dao.get_one([self.box_id, self.process_name])
        box_config.is_on = new_state
        self.bc_dao.update(box_config)

    @valid_action_request
    def mark_for_start(self):
        self._change_state(True)
        return self.reply_ok()

    @valid_action_request
    def mark_for_stop(self):
        self._change_state(False)
        return self.reply_ok()
