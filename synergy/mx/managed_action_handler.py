__author__ = 'Bohdan Mushkevych'

import collections

from synergy.system import time_helper
from synergy.conf import context
from synergy.mx.base_request_handler import valid_action_request
from synergy.mx.abstract_action_handler import AbstractActionHandler
from synergy.mx.tree_node_details import TreeNodeDetails


class ManagedActionHandler(AbstractActionHandler):
    def __init__(self, request, **values):
        super(ManagedActionHandler, self).__init__(request, **values)
        self.process_name = self.request_arguments.get('process_name')
        self.timeperiod = self.request_arguments.get('timeperiod')
        self.is_request_valid = True if self.process_name and self.timeperiod else False

        if self.is_request_valid:
            self.process_name = self.process_name.strip()
            self.timeperiod = self.timeperiod.strip()

    def _get_tree_node(self):
        tree = self.scheduler.timetable.get_tree(self.process_name)
        if tree is None:
            raise UserWarning(f'No Timetable tree is registered for process {self.process_name}')

        time_qualifier = context.process_context[self.process_name].time_qualifier
        self.timeperiod = time_helper.cast_to_time_qualifier(time_qualifier, self.timeperiod)
        node = tree.get_node(self.process_name, self.timeperiod)
        return node

    @AbstractActionHandler.thread_handler.getter
    def thread_handler(self):
        handler_key = self.process_name
        return self.scheduler.managed_handlers[handler_key]

    @AbstractActionHandler.process_entry.getter
    def process_entry(self):
        return self.thread_handler.process_entry

    @AbstractActionHandler.uow_id.getter
    def uow_id(self):
        node = self._get_tree_node()
        return None if not node.job_record else node.job_record.related_unit_of_work

    @valid_action_request
    def reprocess_tree_node(self):
        node = self._get_tree_node()

        msg = f'MX: requesting REPROCESS for {self.process_name} in timeperiod {self.timeperiod}'
        self.scheduler.timetable.add_log_entry(self.process_name, self.timeperiod, msg)
        self.logger.info(msg + ' {')

        tx_context = self.scheduler.timetable.reprocess_tree_node(node)
        self.scheduler.gc.validate()

        resp = collections.defaultdict(dict)
        for process_name, nodes_context in tx_context.items():
            for timeperiod, node in nodes_context.items():
                resp[process_name][timeperiod] = TreeNodeDetails.get_details(node)
        self.logger.info('MX }')
        return resp

    @valid_action_request
    def skip_tree_node(self):
        node = self._get_tree_node()

        msg = f'MX: requesting SKIP for {self.process_name} in timeperiod {self.timeperiod}'
        self.scheduler.timetable.add_log_entry(self.process_name, self.timeperiod, msg)
        self.logger.info(msg + ' {')

        tx_context = self.scheduler.timetable.skip_tree_node(node)
        self.scheduler.gc.validate()

        resp = collections.defaultdict(dict)
        for process_name, nodes_context in tx_context.items():
            for timeperiod, node in nodes_context.items():
                resp[process_name][timeperiod] = TreeNodeDetails.get_details(node)
        self.logger.info('MX }')
        return resp

    @valid_action_request
    def get_event_log(self):
        node = self._get_tree_node()
        return {'event_log': [] if not node.job_record else node.job_record.event_log}
