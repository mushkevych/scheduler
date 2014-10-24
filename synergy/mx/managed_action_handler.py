__author__ = 'Bohdan Mushkevych'

from synergy.db.model.scheduler_managed_entry import SchedulerManagedEntry
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.system import time_helper
from synergy.conf.process_context import ProcessContext
from synergy.mx.mx_decorators import valid_action_request
from synergy.mx.abstract_action_handler import AbstractActionHandler
from synergy.mx.tree_node_details import TreeNodeDetails


class ManagedActionHandler(AbstractActionHandler):
    def __init__(self, mbean, request):
        super(ManagedActionHandler, self).__init__(mbean, request)
        self.process_name = self.request_arguments.get('process_name')
        self.timeperiod = self.request_arguments.get('timeperiod')
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.is_request_valid = self.mbean is not None \
                                and not not self.process_name \
                                and not not self.timeperiod

        if self.is_request_valid:
            self.process_name = self.process_name.strip()
            self.timeperiod = self.timeperiod.strip()

    def _get_tree_node(self):
        timetable = self.mbean.timetable
        tree = timetable.get_tree(self.process_name)
        if tree is None:
            raise UserWarning('No Timetable tree is registered for process %s' % self.process_name)

        time_qualifier = ProcessContext.get_time_qualifier(self.process_name)
        self.timeperiod = time_helper.cast_to_time_qualifier(time_qualifier, self.timeperiod)
        node = tree.get_node_by_process(self.process_name, self.timeperiod)
        return node

    @AbstractActionHandler.scheduler_thread_handler.getter
    def scheduler_thread_handler(self):
        handler_key = self.process_name
        return self.mbean.managed_handlers[handler_key]

    @AbstractActionHandler.scheduler_entry.getter
    def scheduler_entry(self):
        scheduler_entry_obj = self.scheduler_thread_handler.arguments.scheduler_entry_obj
        assert isinstance(scheduler_entry_obj, SchedulerManagedEntry)
        return scheduler_entry_obj

    @valid_action_request
    def action_reprocess(self):
        node = self._get_tree_node()
        self.logger.info('MX (requesting re-process timeperiod %r for %r) {' % (self.timeperiod, self.process_name))
        effected_nodes = node.request_reprocess()

        resp = dict()
        for node in effected_nodes:
            resp[node.timeperiod] = TreeNodeDetails.get_details(self.logger, node)
        self.logger.info('}')
        return resp

    @valid_action_request
    def action_skip(self):
        node = self._get_tree_node()
        self.logger.info('MX (requesting skip timeperiod %r for %r) { ' % (self.timeperiod, self.process_name))
        effected_nodes = node.request_skip()

        resp = dict()
        for node in effected_nodes:
            resp[node.timeperiod] = TreeNodeDetails.get_details(self.logger, node)
        self.logger.info('}')
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
