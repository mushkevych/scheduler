__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, ObjectIdField

PROCESS_NAME = 'process_name'
STATE = 'state'
TRIGGER_TIME = 'trigger_time'
STATE_MACHINE_NAME = 'state_machine_name'
BLOCKING_TYPE = 'blocking_type'

STATE_ON = 'state_on'
STATE_OFF = 'state_off'


class SchedulerManagedEntry(BaseDocument):
    """ Class presents single configuration entry for scheduler managed (i.e. - non-freerun) processes. """
    db_id = ObjectIdField('_id', null=True)
    process_name = StringField(PROCESS_NAME)
    trigger_time = StringField(TRIGGER_TIME)
    state = StringField(STATE, choices=[STATE_ON, STATE_OFF])
    state_machine_name = StringField(STATE_MACHINE_NAME)
    blocking_type = StringField(BLOCKING_TYPE)

    @BaseDocument.key.getter
    def key(self):
        return self.process_name

    @key.setter
    def key(self, value):
        self.process_name = value
