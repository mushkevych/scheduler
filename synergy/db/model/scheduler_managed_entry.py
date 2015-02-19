__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField

PROCESS_NAME = 'process_name'
STATE = 'state'
TRIGGER_TIME = 'trigger_time'
PIPELINE_NAME = 'pipeline_name'
PROCESS_TYPE = 'process_type'

STATE_ON = 'state_on'
STATE_OFF = 'state_off'


class SchedulerManagedEntry(BaseDocument):
    """ Class presents single configuration entry for scheduler managed (i.e. - non-freerun) processes. """
    process_name = StringField(PROCESS_NAME)
    trigger_time = StringField(TRIGGER_TIME)
    state = StringField(STATE, choices=[STATE_ON, STATE_OFF])
    state_machine_name = StringField(PIPELINE_NAME)
    process_type = StringField(PROCESS_TYPE)

    @property
    def key(self):
        return self.process_name

    @key.setter
    def key(self, value):
        self.process_name = value
