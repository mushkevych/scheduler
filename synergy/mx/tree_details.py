__author__ = 'Bohdan Mushkevych'

from werkzeug.utils import cached_property
from synergy.mx.rest_model_factory import create_rest_timetable_tree, create_rest_managed_scheduler_entry


class TreeDetails(object):
    def __init__(self, mbean, request):
        self.mbean = mbean
        self.logger = self.mbean.logger
        self.referrer = request.referrer

    def _get_tree_details(self, tree_name):
        tree_obj = self.mbean.timetable.trees[tree_name]
        rest_tree = create_rest_timetable_tree(self.mbean.timetable, tree_obj)

        for process_name in tree_obj.process_hierarchy:
            thread_handler = self.mbean.managed_handlers[process_name]
            rest_process = create_rest_managed_scheduler_entry(thread_handler, self.mbean.timetable)
            rest_tree.processes[process_name] = rest_process.document
        return rest_tree

    @cached_property
    def timetable_entries(self):
        trees = dict()
        timetable = self.mbean.timetable
        try:
            for tree_name in timetable.trees:
                rest_tree = self._get_tree_details(tree_name)
                trees[rest_tree.tree_name] = rest_tree.document

        except Exception as e:
            self.logger.error('MX Exception %s' % str(e), exc_info=True)

        return trees

    @cached_property
    def mx_page_entries(self):
        resp = dict()
        timetable = self.mbean.timetable

        for tree_name, tree in timetable.trees.items():
            if tree.mx_page in self.referrer:
                rest_tree = self._get_tree_details(tree_name)
                resp[tree.tree_name] = rest_tree.document

        return resp
