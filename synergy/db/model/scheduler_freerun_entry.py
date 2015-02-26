__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, DictField, ListField, ObjectIdField

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


class SchedulerFreerunEntry(BaseDocument):
    """ Class presents single configuration entry for the freerun process/bash_driver . """

    db_id = ObjectIdField('_id', null=True)
    process_name = StringField(PROCESS_NAME)
    entry_name = StringField(ENTRY_NAME)
    description = StringField(DESCRIPTION)
    trigger_time = StringField(TRIGGER_TIME)
    state = StringField(STATE, choices=[STATE_ON, STATE_OFF])
    arguments = DictField(ARGUMENTS)
    log = ListField(HISTORIC_LOG)
    related_unit_of_work = ObjectIdField(RELATED_UNIT_OF_WORK)

    @BaseDocument.key.getter
    def key(self):
        return self.process_name, self.entry_name

    @key.setter
    def key(self, value):
        self.process_name = value[0]
        self.entry_name = value[1]
