__author__ = 'Bohdan Mushkevych'

from odm.fields import BooleanField, StringField, DictField, ListField, NestedDocumentField
from odm.document import BaseDocument


FIELD_PROCESS_NAME = 'process_name'         # process name
FIELD_ENTRY_NAME = 'entry_name'             # name of the schedulable
FIELD_IS_ON = 'is_on'                       # whether the trigger is expected to be in On/Off state
FIELD_IS_ALIVE = 'is_alive'                 # actual state of the trigger
FIELD_TRIGGER_FREQUENCY = 'trigger_frequency'   # human-readable trigger frequency: every XXX or at YYY
FIELD_NEXT_RUN_IN = 'next_run_in'           # duration until the next trigger event in HH:MM:SS format
FIELD_ARGUMENTS = 'arguments'               # arguments for the schedulable
FIELD_NEXT_TIMEPERIOD = 'next_timeperiod'   # Synergy timeperiod format
FIELD_STATE_MACHINE = 'state_machine'
FIELD_PROCESS_TYPE = 'process_type'
FIELD_RUN_ON_ACTIVE_TIMEPERIOD = 'run_on_active_timeperiod'
FIELD_DEPENDENT_ON = 'dependent_on'
FIELD_DEPENDANT_TREES = 'dependant_trees'
FIELD_TREE_NAME = 'tree_name'
FIELD_MX_PAGE = 'mx_page'
FIELD_MX_NAME = 'mx_page_name'
FIELD_REPROCESSING_QUEUE = 'reprocessing_queue'

FIELD_NUMBER_OF_CHILDREN = 'number_of_children'
FIELD_NUMBER_OF_FAILED_CALLS = 'number_of_failed_calls'
FIELD_PROCESSES = 'processes'
FIELD_TIMEPERIOD = 'timeperiod'                 # Synergy timeperiod format
FIELD_TIME_QUALIFIER = 'time_qualifier'
FIELD_STATE = 'state'
FIELD_CHILDREN = 'children'
FIELD_NODE = 'node'


class RestFreerunSchedulerEntry(BaseDocument):
    is_on = BooleanField(FIELD_IS_ON)
    is_alive = BooleanField(FIELD_IS_ALIVE)
    process_name = StringField(FIELD_PROCESS_NAME)
    entry_name = StringField(FIELD_ENTRY_NAME)
    trigger_frequency = StringField(FIELD_TRIGGER_FREQUENCY)
    next_run_in = StringField(FIELD_NEXT_RUN_IN)
    arguments = DictField(FIELD_ARGUMENTS)


class RestManagedSchedulerEntry(BaseDocument):
    is_on = BooleanField(FIELD_IS_ON)
    is_alive = BooleanField(FIELD_IS_ALIVE)
    process_name = StringField(FIELD_PROCESS_NAME)
    trigger_frequency = StringField(FIELD_TRIGGER_FREQUENCY)
    next_run_in = StringField(FIELD_NEXT_RUN_IN)
    next_timeperiod = StringField(FIELD_NEXT_TIMEPERIOD)


class RestProcess(BaseDocument):
    process_name = StringField(FIELD_PROCESS_NAME)
    time_qualifier = StringField(FIELD_TIME_QUALIFIER)
    state_machine = StringField(FIELD_STATE_MACHINE)
    process_type = StringField(FIELD_PROCESS_TYPE)
    run_on_active_timeperiod = BooleanField(FIELD_RUN_ON_ACTIVE_TIMEPERIOD)
    reprocessing_queue = ListField(FIELD_REPROCESSING_QUEUE)
    next_timeperiod = StringField(FIELD_NEXT_TIMEPERIOD)


class RestTimetableTree(BaseDocument):
    tree_name = StringField(FIELD_TREE_NAME)
    mx_page = StringField(FIELD_MX_PAGE)
    mx_name = StringField(FIELD_MX_NAME)
    dependent_on = ListField(FIELD_DEPENDENT_ON)
    dependant_trees = ListField(FIELD_DEPENDANT_TREES)
    processes = DictField(FIELD_PROCESSES)


class RestJob(BaseDocument):
    process_name = StringField(FIELD_PROCESS_NAME)
    time_qualifier = StringField(FIELD_TIME_QUALIFIER)
    timeperiod = StringField(FIELD_TIMEPERIOD)
    number_of_children = StringField(FIELD_NUMBER_OF_CHILDREN)
    number_of_failed_calls = StringField(FIELD_NUMBER_OF_FAILED_CALLS)
    state = StringField(FIELD_STATE)


class RestTimetableTreeNode(BaseDocument):
    node = NestedDocumentField(FIELD_NODE, RestJob, null=True)
    children = DictField(FIELD_CHILDREN)
