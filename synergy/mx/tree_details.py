__author__ = 'Bohdan Mushkevych'

from werkzeug.utils import cached_property

from synergy.conf import context
from synergy.mx.rest_models import RestTimetableTree, RestProcess


class TreeDetails(object):
    def __init__(self, mbean, request):
        self.mbean = mbean
        self.referrer = request.referrer
        self.logger = self.mbean.logger

    def _list_of_dependant_trees(self, tree_obj):
        trees = self.mbean.timetable._find_dependant_trees(tree_obj)
        return [x.full_name for x in trees]

    def _get_reprocessing_details(self, process_name):
        resp = []
        per_process = self.mbean.timetable.reprocess.get(process_name)
        if per_process is not None:
            resp = sorted(per_process.keys())
        return resp

    def _get_tree_details(self, tree_name):
        tree_obj = self.mbean.timetable.trees[tree_name]
        context_entry = context.timetable_context[tree_name]

        rest_tree = RestTimetableTree()
        rest_tree.tree_name = tree_name
        rest_tree.mx_page = tree_obj.mx_page
        rest_tree.mx_name = tree_obj.mx_name
        rest_tree.dependent_on = context_entry.dependent_on
        rest_tree.dependant_trees = self._list_of_dependant_trees(tree_obj)

        for process_name in context_entry.enclosed_processes:
            process_obj = context.process_context[process_name]
            rest_process = RestProcess(
                process_name=process_name,
                time_qualifier=process_obj.time_qualifier,
                state_machine=process_obj.state_machine_name,
                process_type=process_obj.process_type,
                run_on_active_timeperiod=process_obj.run_on_active_timeperiod,
                reprocessing_queue=self._get_reprocessing_details(process_name),
                next_timeperiod=self.mbean.timetable.get_next_job_record(process_name).timeperiod
            )
            rest_tree.processes[process_name] = rest_process.document
        return rest_tree

    @cached_property
    def timetable_entries(self):
        trees = dict()
        try:
            sorter_keys = sorted(context.timetable_context.keys())
            for tree_name in sorter_keys:
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
                resp[tree.mx_name] = rest_tree.document

        return resp
