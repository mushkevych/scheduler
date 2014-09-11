__author__ = 'Bohdan Mushkevych'

from werkzeug.utils import cached_property

import context
from system.process_context import ProcessContext


# Scheduler Entries Details tab
class TimetableDetails(object):
    def __init__(self, mbean):
        self.mbean = mbean
        self.logger = self.mbean.logger

    @cached_property
    def entries(self):
        list_of_trees = []
        try:
            sorter_keys = sorted(self.mbean.timetable.keys())
            for tree_name in sorter_keys:
                tree_instance = self.mbean.timetable[tree_name]

                tree_row = list()
                tree_row.append(tree_name)                                              # index 0
                tree_row.append(tree_instance.mx_page)                                  # index 1

                processes = dict()                                                      # index 2
                context_entry = context.timetable_context[tree_name]
                for process_name in context_entry.enclosed_processes:
                    process_details = [process_name,                                                 # index x0
                                       ProcessContext.get_time_qualifier(process_name),              # index x1
                                       context.timetable_context[process_name].state_machine_name,   # index x2
                                       ProcessContext.get_process_type(process_name),                # index x3
                                       context_entry.dependent_on,                                   # index x4
                                       self.mbean.timetable._find_dependant_trees(tree_instance)]    # index x5
                    processes[process_name] = process_details
                tree_row.append(processes)

                list_of_trees.append(tree_row)
        except Exception as e:
            self.logger.error('MX Exception %s' % str(e), exc_info=True)

        return list_of_trees
