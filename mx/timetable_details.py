__author__ = 'Bohdan Mushkevych'

from werkzeug.utils import cached_property

import context
from system.process_context import ProcessContext


# Scheduler Entries Details tab
class TimetableDetails(object):
    def __init__(self, mbean):
        self.mbean = mbean
        self.logger = self.mbean.logger

    def _list_of_dependant_trees(self, tree_obj):
        trees = self.mbean.timetable._find_dependant_trees(tree_obj)
        return [x.full_name for x in trees]

    def _state_machine_name(self, process_name):
        return self.mbean.thread_handlers[process_name].args[1].state_machine_name

    @cached_property
    def entries(self):
        list_of_trees = []
        try:
            sorter_keys = sorted(context.timetable_context.keys())
            for tree_name in sorter_keys:
                tree_obj = self.mbean.timetable.trees[tree_name]

                tree_row = list()
                tree_row.append(tree_name)                                              # index 0
                tree_row.append(tree_obj.mx_page)                                       # index 1
                tree_row.append(tree_obj.mx_name)                                       # index 2

                processes = dict()                                                      # index 3
                context_entry = context.timetable_context[tree_name]
                for process_name in context_entry.enclosed_processes:
                    process_details = [process_name,                                                 # index x0
                                       ProcessContext.get_time_qualifier(process_name),              # index x1
                                       self._state_machine_name(process_name),                       # index x2
                                       ProcessContext.get_process_type(process_name),                # index x3
                                       context_entry.dependent_on,                                   # index x4
                                       self._list_of_dependant_trees(tree_obj)]                      # index x5
                    processes[process_name] = process_details
                tree_row.append(processes)

                list_of_trees.append(tree_row)
        except Exception as e:
            self.logger.error('MX Exception %s' % str(e), exc_info=True)

        return list_of_trees
