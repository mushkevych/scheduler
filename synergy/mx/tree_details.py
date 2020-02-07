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

    def _all_trees(self):
        resp = dict()
        for tree_name in self.scheduler.timetable.trees:
            resp[tree_name] = self._get_tree_details(tree_name).document
        return resp

    def _mx_page_trees(self, mx_page):
        """ return trees assigned to given MX Page """
        resp = dict()
        for tree_name, tree in self.scheduler.timetable.trees.items():
            if tree.mx_page == mx_page:
                resp[tree_name] = self._get_tree_details(tree_name).document
        return resp

    @property
    def trees(self):
        mx_page = self.request_arguments.get('mx_page')
        return self._mx_page_trees(mx_page) if mx_page else self._all_trees()

    @cached_property
    def tree_details(self):
        tree_name = self.request_arguments.get('tree_name')
        if tree_name:
            return self._get_tree_details(tree_name).document
        else:
            return dict()
