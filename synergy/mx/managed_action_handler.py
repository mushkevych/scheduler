__author__ = 'Bohdan Mushkevych'

from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
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
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.is_request_valid = True if self.process_name and self.timeperiod else False

        if self.is_request_valid:
            self.process_name = self.process_name.strip()
            self.timeperiod = self.timeperiod.strip()

    def _get_tree_node(self):
        tree = self.scheduler.timetable.get_tree(self.process_name)
        if tree is None:
            raise UserWarning('No Timetable tree is registered for process %s' % self.process_name)

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

    @valid_action_request
    def action_reprocess(self):
        node = self._get_tree_node()

        msg = 'MX: requesting REPROCESS for %s in timeperiod %s' % (self.process_name, self.timeperiod)
        self.scheduler.timetable.add_log_entry(self.process_name, self.timeperiod, msg)
        self.logger.info(msg + ' {')

        effected_nodes = node.request_reprocess()

        resp = dict()
        for node in effected_nodes:
            resp[node.timeperiod] = TreeNodeDetails.get_details(node)
        self.logger.info('MX }')
        return resp

    @valid_action_request
    def action_skip(self):
        node = self._get_tree_node()

        msg = 'MX: requesting SKIP for %s in timeperiod %s' % (self.process_name, self.timeperiod)
        self.scheduler.timetable.add_log_entry(self.process_name, self.timeperiod, msg)
        self.logger.info(msg + ' {')

        effected_nodes = node.request_skip()

        resp = dict()
        for node in effected_nodes:
            resp[node.timeperiod] = TreeNodeDetails.get_details(node)
        self.logger.info('MX }')
        return resp

    @valid_action_request
    def action_get_uow(self):
        node = self._get_tree_node()

        uow_id = node.job_record.related_unit_of_work
        if uow_id is None:
            resp = {'response': 'no related unit_of_work'}
        else:
            resp = self.uow_dao.get_one(uow_id).document
            for key in resp:
                resp[key] = str(resp[key])

        return resp

    @valid_action_request
    def action_get_log(self):
        node = self._get_tree_node()
        return {'log': node.job_record.log}
