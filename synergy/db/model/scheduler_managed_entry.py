__author__ = 'Bohdan Mushkevych'

from synergy.db.model.base_model import *

PROCESS_NAME = 'process_name'
STATE = 'state'
TRIGGER_TIME = 'trigger_time'
PIPELINE_NAME = 'pipeline_name'
PROCESS_TYPE = 'process_type'

STATE_ON = 'state_on'
STATE_OFF = 'state_off'


class SchedulerManagedEntry(BaseModel):
    """ Class presents single configuration entry for scheduler managed (i.e. - non-freerun) processes. """

    def __init__(self, document=None):
        super(SchedulerManagedEntry, self).__init__(document)

    @property
    def key(self):
        return self.data[PROCESS_NAME]

    @key.setter
    def key(self, value):
        self.data[PROCESS_NAME] = value

    @property
    def process_name(self):
        return self.data[PROCESS_NAME]

    @process_name.setter
    def process_name(self, value):
        self.data[PROCESS_NAME] = value

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
    def state_machine_name(self):
        return self.data[PIPELINE_NAME]

    @state_machine_name.setter
    def state_machine_name(self, value):
        self.data[PIPELINE_NAME] = value

    @property
    def process_type(self):
        return self.data[PROCESS_TYPE]

    @process_type.setter
    def process_type(self, value):
        self.data[PROCESS_TYPE] = value
