__author__ = 'Bohdan Mushkevych'

from synergy.db.model.base_model import *

PROCESS_NAME = 'process_name'           # name of the process to handle the schedulables
ENTRY_NAME = 'entry_name'               # name of the schedulable
DESCRIPTION = 'description'             # description of the schedulable
STATE = 'state'                         # either :STATE_ON or :STATE_OFF
TRIGGER_TIME = 'trigger_time'           # either 'at DoW-HH:MM' or 'every XXX'
ARGUMENTS = 'arguments'                 # arguments that defines a job (host, script to run, etc)

HISTORIC_LOG = 'historic_log'           # contains list of MAX_NUMBER_OF_LOG_ENTRIES last log messages
MAX_NUMBER_OF_LOG_ENTRIES = 64
RELATED_UNIT_OF_WORK = 'related_unit_of_work'

STATE_ON = 'state_on'
STATE_OFF = 'state_off'


class SchedulerFreerunEntry(BaseModel):
    """ Class presents single configuration entry for the freerun process/bash_driver . """

    def __init__(self, document=None):
        super(SchedulerFreerunEntry, self).__init__(document)

    @property
    def key(self):
        return self.process_name, self.entry_name

    @key.setter
    def key(self, value):
        assert not isinstance(value, str)
        self.process_name = value[0]
        self.entry_name = value[1]

    @property
    def process_name(self):
        return self.data[PROCESS_NAME]

    @process_name.setter
    def process_name(self, value):
        self.data[PROCESS_NAME] = value

    @property
    def entry_name(self):
        return self.data[ENTRY_NAME]

    @entry_name.setter
    def entry_name(self, value):
        self.data[ENTRY_NAME] = value

    @property
    def description(self):
        return self.data[DESCRIPTION]

    @description.setter
    def description(self, value):
        self.data[DESCRIPTION] = value

    @property
    def trigger_time(self):
        return self.data[TRIGGER_TIME]

    @trigger_time.setter
    def trigger_time(self, value):
        self.data[TRIGGER_TIME] = value

    @property
    def state(self):
        return self.data[STATE]

    @state.setter
    def state(self, value):
        if value not in [STATE_ON, STATE_OFF]:
            raise ValueError('incorrect state for process %r' % value)
        self.data[STATE] = value

    @property
    def arguments(self):
        return self.data.get(ARGUMENTS, dict())

    @arguments.setter
    def arguments(self, value):
        if not isinstance(value, dict):
            raise ValueError('incorrect arguments format %r. must be dict' % value.__class__.__name__)
        self.data[ARGUMENTS] = value

    @property
    def log(self):
        if HISTORIC_LOG not in self.data:
            self.data[HISTORIC_LOG] = []
        return self.data[HISTORIC_LOG]

    @log.setter
    def log(self, value):
        self.data[HISTORIC_LOG] = value

    @property
    def related_unit_of_work(self):
        return self.data.get(RELATED_UNIT_OF_WORK)

    @related_unit_of_work.setter
    def related_unit_of_work(self, value):
        self.data[RELATED_UNIT_OF_WORK] = value
