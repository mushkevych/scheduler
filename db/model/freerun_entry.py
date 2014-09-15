__author__ = 'Bohdan Mushkevych'

from db.model.base_model import *

ENTRY_NAME = 'entry_name'
DESCRIPTION = 'description'
STATE = 'state'
TRIGGER_TIME = 'trigger_time'
ARGUMENTS = 'arguments'                # arguments that defines a job (host, script to run, etc)

STATE_ON = 'state_on'
STATE_OFF = 'state_off'


class FreerunEntry(BaseModel):
    """ Class presents single configuration entry for the freerun process/bash_driver . """

    def __init__(self, document=None):
        super(FreerunEntry, self).__init__(document)

    @property
    def key(self):
        return self.data[ENTRY_NAME]

    @key.setter
    def key(self, value):
        self.data[ENTRY_NAME] = value

    @property
    def process_name(self):
        return self.data[ENTRY_NAME]

    @process_name.setter
    def process_name(self, value):
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
    def process_state(self):
        return self.data[STATE]

    @process_state.setter
    def process_state(self, value):
        if value not in [STATE_ON, STATE_OFF]:
            raise ValueError('incorrect state for process %r' % value)
        self.data[STATE] = value

    @property
    def arguments(self):
        return self.data.get(ARGUMENTS)

    @arguments.setter
    def arguments(self, value):
        self.data[ARGUMENTS] = value
