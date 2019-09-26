__author__ = 'Bohdan Mushkevych'

from collections import OrderedDict

from synergy.conf import context
from synergy.db.model.managed_process_entry import ManagedProcessEntry
from synergy.system.time_qualifier import *
from synergy.system.time_helper import cast_to_time_qualifier
from synergy.system.timeperiod_dict import TimeperiodDict


class HierarchyEntry(object):
    def __init__(self, hierarchy, parent, process_entry: ManagedProcessEntry):
        self.hierarchy = hierarchy
        self.parent = parent
        self.process_entry = process_entry
        self.timeperiod_dict = TimeperiodDict(process_entry.time_qualifier, process_entry.time_grouping)

    def cast_timeperiod(self, timeperiod):
        return cast_to_time_qualifier(self.process_entry.time_qualifier, timeperiod)


class ProcessHierarchy(object):
    def __init__(self, *process_names):
        self.entries = OrderedDict()
        self.qualifiers = OrderedDict()

        top_node = None
        process_entries = [context.process_context[process_name] for process_name in process_names]

        sorted_process_entries = sorted(process_entries, key=lambda x: QUALIFIER_DICT[x.time_qualifier], reverse=True)
        for process_entry in sorted_process_entries:
            assert isinstance(process_entry, ManagedProcessEntry)
            entry = HierarchyEntry(self, top_node, process_entry)
            top_node = entry
            self.entries[process_entry.process_name] = entry
            self.qualifiers[process_entry.time_qualifier] = entry

    def __contains__(self, value):
        """
        :param value: process name
        :return: True if a hierarchy entry for the process_entry with the given name is registered in this hierarchy;
                 False otherwise
        """
        return value in self.entries

    def __getitem__(self, key):
        """
        :param key: process name
        :return: associated hierarchy entry of HierarchyEntry type
        """
        return self.entries[key]

    def __iter__(self):
        """ for x in self
        :return process_names in descending order of their time_qualifier: yearly->monthly->daily->hourly """
        return iter(self.entries)

    def __str__(self):
        msg = 'Process Hierarchy: '
        for process_name, hierarchy_entry in self.entries:
            msg += '{1}->{0} '.format(process_name, hierarchy_entry.process_entry.time_qualifier)
        return msg

    def has_qualifier(self, qualifier):
        """
        :param qualifier: time_qualifier
        :return: True if a HierarchyEntry with given time_qualifier is registered in this hierarchy; False otherwise
        """
        return qualifier in self.qualifiers

    def get_by_qualifier(self, qualifier):
        """
        :param qualifier: time_qualifier of the searched process
        :return: associated entry of HierarchyEntry type
                or None if no process with given time_qualifier is registered in this hierarchy
        """
        return self.qualifiers.get(qualifier, None)

    def get_child_by_qualifier(self, parent_qualifier):
        """
        :param parent_qualifier: time_qualifier of the parent process
        :return: <HierarchyEntry> child entry to the HierarchyEntry associated with the parent_qualifier
                or None if the given parent_qualifier is not registered in this hierarchy
                or None if the given parent_qualifier is the bottom process
        """
        if parent_qualifier not in self.qualifiers:
            return None

        process_qualifiers = list(self.qualifiers)
        if parent_qualifier == process_qualifiers[-1]:
            return None

        parent_index = process_qualifiers.index(parent_qualifier)
        return self.qualifiers[process_qualifiers[parent_index + 1]]

    @property
    def top_process(self):
        """ :return: <ManagedProcessEntry> of the hierarchy's top entry """
        key = next(iter(self.entries))
        return self.entries[key].process_entry

    @property
    def bottom_process(self):
        """ :return: <ManagedProcessEntry> of the hierarchy's bottom entry """
        key = next(reversed(self.entries))
        return self.entries[key].process_entry
