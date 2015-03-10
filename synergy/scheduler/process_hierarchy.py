__author__ = 'Bohdan Mushkevych'

from collections import OrderedDict

from synergy.db.model.managed_process_entry import ManagedProcessEntry
from synergy.system.time_qualifier import *


class HierarchyEntry(object):
    def __init__(self, hierarchy, parent, process_entry):
        self.hierarchy = hierarchy
        self.parent = parent
        self.process_entry = process_entry


class Hierarchy(object):
    def __init__(self, *process_entries):
        self.entries = OrderedDict()
        self.qualifiers = OrderedDict()

        top_node = None
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
        :return: True if a process_entry with the given name is registered in this hierarchy; False otherwise
        """
        return value in self.entries

    def __getitem__(self, key):
        """
        :param key: process name
        :return: associated process_entry of ManagedProcessEntry type
        """
        return self.entries[key]

    def __iter__(self):
        """ for x in self """
        return iter(self.entries)

    def __str__(self):
        msg = 'Process Hierarchy: '
        for process_name, hierarchy_entry in self.entries:
            msg += '{1}->{0} '.format(process_name, hierarchy_entry.process_entry.time_qualifier)
        return msg

    def has_qualifier(self, qualifier):
        """
        :param qualifier: time_qualifier
        :return: True if a process_entry with given time_qualifier is registered in this hierarchy; False otherwise
        """
        return qualifier in self.qualifiers

    def get_by_qualifier(self, qualifier):
        """
        :param qualifier: time_qualifier of the searched process
        :return: associated process_entry of ManagedProcessEntry type
                or None if no process with given time_qualifier is registered in this hierarchy
        """
        return self.qualifiers.get(qualifier, None)
