__author__ = 'Bohdan Mushkevych'

from odm.fields import BooleanField, StringField, DictField, ListField, NestedDocumentField
from odm.document import BaseDocument

from synergy.db.model.job import Job
from synergy.db.model.managed_process_entry import ManagedProcessEntry
from synergy.db.model.freerun_process_entry import FreerunProcessEntry
from synergy.db.model.timetable_tree_entry import TimetableTreeEntry


FIELD_IS_ALIVE = 'is_alive'                 # actual state of the trigger
FIELD_NEXT_RUN_IN = 'next_run_in'           # duration until the next trigger event in HH:MM:SS format
FIELD_NEXT_TIMEPERIOD = 'next_timeperiod'   # Synergy timeperiod format
FIELD_DEPENDANT_TREES = 'dependant_trees'
FIELD_SORTED_PROCESS_NAMES = 'sorted_process_names'     # process names sorted by their time_qualifier
FIELD_REPROCESSING_QUEUE = 'reprocessing_queue'

FIELD_NUMBER_OF_CHILDREN = 'number_of_children'
FIELD_PROCESSES = 'processes'
FIELD_TIME_QUALIFIER = 'time_qualifier'
FIELD_CHILDREN = 'children'
FIELD_NODE = 'node'


class RestFreerunSchedulerEntry(FreerunProcessEntry):
    is_alive = BooleanField(FIELD_IS_ALIVE)
    next_run_in = StringField(FIELD_NEXT_RUN_IN)


class RestManagedSchedulerEntry(ManagedProcessEntry):
    is_alive = BooleanField(FIELD_IS_ALIVE)
    next_run_in = StringField(FIELD_NEXT_RUN_IN)
    next_timeperiod = StringField(FIELD_NEXT_TIMEPERIOD)
    reprocessing_queue = ListField(FIELD_REPROCESSING_QUEUE)


class RestTimetableTree(TimetableTreeEntry):
    dependant_trees = ListField(FIELD_DEPENDANT_TREES)
    sorted_process_names = ListField(FIELD_SORTED_PROCESS_NAMES)
    processes = DictField(FIELD_PROCESSES)


class RestJob(Job):
    time_qualifier = StringField(FIELD_TIME_QUALIFIER)
    number_of_children = StringField(FIELD_NUMBER_OF_CHILDREN)


class RestTimetableTreeNode(BaseDocument):
    node = NestedDocumentField(FIELD_NODE, RestJob, null=True)
    children = DictField(FIELD_CHILDREN)
