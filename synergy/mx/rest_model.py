__author__ = 'Bohdan Mushkevych'

from odm.fields import BooleanField, StringField, DictField, ListField, NestedDocumentField
from odm.document import BaseDocument

from synergy.db.model.job import Job
from synergy.db.model.managed_process_entry import ManagedProcessEntry
from synergy.db.model.freerun_process_entry import FreerunProcessEntry
from synergy.db.model.timetable_tree_entry import TimetableTreeEntry


class RestFreerunSchedulerEntry(FreerunProcessEntry):
    is_alive = BooleanField()
    next_run_in = StringField()


class RestManagedSchedulerEntry(ManagedProcessEntry):
    is_alive = BooleanField()
    next_run_in = StringField()
    next_timeperiod = StringField()
    reprocessing_queue = ListField()


class RestTimetableTree(TimetableTreeEntry):
    dependant_trees = ListField()
    sorted_process_names = ListField()
    processes = DictField()


class RestJob(Job):
    time_qualifier = StringField()
    number_of_children = StringField()


class RestTimetableTreeNode(BaseDocument):
    node = NestedDocumentField(RestJob, null=True)
    children = DictField()
