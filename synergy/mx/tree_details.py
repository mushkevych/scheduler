__author__ = 'Bohdan Mushkevych'

from werkzeug.utils import cached_property

from synergy.mx.rest_models import RestTimetableTree, RestManagedSchedulerEntry
from synergy.mx.scheduler_entries import handler_next_timeperiod, handler_next_run


class TreeDetails(object):
    def __init__(self, mbean, request):
        self.mbean = mbean
        self.logger = self.mbean.logger
        self.referrer = request.referrer

    def _list_of_dependant_trees(self, tree_obj):
        trees = self.mbean.timetable._find_dependant_trees(tree_obj)
        return [x.tree_name for x in trees]

    def _get_reprocessing_details(self, process_name):
        resp = []
        per_process = self.mbean.timetable.reprocess.get(process_name)
        if per_process is not None:
            resp = sorted(per_process.keys())
        return resp

    def _get_tree_details(self, tree_name):
        tree_obj = self.mbean.timetable.trees[tree_name]

        rest_tree = RestTimetableTree(tree_name=tree_name,
                                      mx_page=tree_obj.mx_page,
                                      mx_name=tree_obj.mx_name,
                                      dependent_on=[tree.tree_name for tree in tree_obj.dependent_on],
                                      dependant_trees=self._list_of_dependant_trees(tree_obj),
                                      sorted_process_names=[x for x in tree_obj.process_hierarchy])

        for process_name in tree_obj.process_hierarchy:
            process_entry = tree_obj.process_hierarchy[process_name].process_entry
            thread_handler = self.mbean.managed_handlers[process_name]

            rest_process = RestManagedSchedulerEntry(
                process_name=process_name,
                state=process_entry.state,
                is_on=process_entry.is_on,
                is_active=thread_handler.is_active,
                time_qualifier=process_entry.time_qualifier,
                state_machine_name=process_entry.state_machine_name,
                process_type=process_entry.process_type,
                blocking_type=process_entry.blocking_type,
                run_on_active_timeperiod=process_entry.run_on_active_timeperiod,
                reprocessing_queue=self._get_reprocessing_details(process_name),
                next_timeperiod=handler_next_timeperiod(self.mbean.timetable, process_name),
                next_run_in=handler_next_run(thread_handler),
                trigger_frequency=process_entry.trigger_frequency
            )
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
