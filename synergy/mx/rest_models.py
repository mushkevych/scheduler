__author__ = 'Bohdan Mushkevych'

from odm.fields import BooleanField, StringField, DictField, ListField, NestedDocumentField, IntegerField
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

FIELD_YEARLY = 'yearly'
FIELD_MONTHLY = 'monthly'
FIELD_DAILY = 'daily'
FIELD_HOURLY = 'hourly'
FIELD_LINEAR = 'linear'
FIELD_NUMBER_OF_LEVELS = 'number_of_levels'
FIELD_REPROCESSING_QUEUES = 'reprocessing_queues'
FIELD_PROCESSES = 'processes'
FIELD_NEXT_TIMEPERIODS = 'next_timeperiods'     # Synergy timeperiod format
# FIELD_PROCESS_TYPES = 'process_types'
FIELD_TIME_QUALIFIER = 'time_qualifier'


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


class ReprocessingQueue(BaseDocument):
    yearly = ListField(FIELD_YEARLY)
    monthly = ListField(FIELD_MONTHLY)
    daily = ListField(FIELD_DAILY)
    hourly = ListField(FIELD_HOURLY)
    linear = ListField(FIELD_LINEAR)


class QualifierBasedCriteria(BaseDocument):
    yearly = StringField(FIELD_YEARLY)
    monthly = StringField(FIELD_MONTHLY)
    daily = StringField(FIELD_DAILY)
    hourly = StringField(FIELD_HOURLY)
    linear = StringField(FIELD_LINEAR)


class RestTreeDetail(BaseDocument):
    reprocessing_queues = NestedDocumentField(FIELD_REPROCESSING_QUEUES, ReprocessingQueue)
    processes = NestedDocumentField(FIELD_PROCESSES, QualifierBasedCriteria)
    next_timeperiods = NestedDocumentField(FIELD_NEXT_TIMEPERIODS, QualifierBasedCriteria)
    number_of_levels = IntegerField(FIELD_NUMBER_OF_LEVELS)


class RestProcess(BaseDocument):
    process_name = StringField(FIELD_PROCESS_NAME)
    time_qualifier = StringField(FIELD_TIME_QUALIFIER)
    state_machine = StringField(FIELD_STATE_MACHINE)
    process_type = StringField(FIELD_PROCESS_TYPE)
    run_on_active_timeperiod = BooleanField(FIELD_RUN_ON_ACTIVE_TIMEPERIOD)
    reprocessing_queue = ListField(FIELD_REPROCESSING_QUEUE)


class RestTimetableTree(BaseDocument):
    tree_name = StringField(FIELD_TREE_NAME)
    mx_page = StringField(FIELD_MX_PAGE)
    mx_name = StringField(FIELD_MX_NAME)
    dependent_on = ListField(FIELD_DEPENDENT_ON)
    dependant_trees = ListField(FIELD_DEPENDANT_TREES)
    processes = DictField(FIELD_PROCESSES)
