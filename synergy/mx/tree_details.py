__author__ = 'Bohdan Mushkevych'

from werkzeug.utils import cached_property
from synergy.mx.base_request_handler import BaseRequestHandler
from synergy.mx.rest_model_factory import create_rest_timetable_tree, create_rest_managed_scheduler_entry


class TreeDetails(BaseRequestHandler):
    def __init__(self, request, **values):
        super(TreeDetails, self).__init__(request, **values)

    def _get_tree_details(self, tree_name):
        tree_obj = self.scheduler.timetable.trees[tree_name]
        rest_tree = create_rest_timetable_tree(self.scheduler.timetable, tree_obj)

        for process_name in tree_obj.process_hierarchy:
            thread_handler = self.scheduler.managed_handlers[process_name]
            rest_process = create_rest_managed_scheduler_entry(thread_handler,
                                                               self.scheduler.timetable,
                                                               self.scheduler.gc)
            rest_tree.processes[process_name] = rest_process.document
        return rest_tree

    @cached_property
    def tree_details(self):
        tree_name = self.request.args.get('tree_name')
        if tree_name:
            return self._get_tree_details(tree_name).document
        else:
            return dict()

    # @cached_property
    def mx_page_trees(self, mx_page):
        """ return trees assigned to given MX Page """
        resp = dict()
        for tree_name, tree in self.scheduler.timetable.trees.items():
            if tree.mx_page == mx_page:
                rest_tree = self._get_tree_details(tree_name)
                resp[tree.tree_name] = rest_tree.document
        return resp
