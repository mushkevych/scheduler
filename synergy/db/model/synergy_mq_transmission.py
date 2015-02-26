__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, ObjectIdField

PROCESS_NAME = 'process_name'           # name of the process to handle the schedulables
ENTRY_NAME = 'entry_name'               # name of the schedulable, if applicable
UNIT_OF_WORK_ID = 'unit_of_work_id'     # associated Unit Of Work, if applicable


class SynergyMqTransmission(BaseDocument):
    """ Non-persistent model. Instance of this class presents either:
     - single request from Synergy Scheduler to any worker
     - response/report from the worker to the Synergy Scheduler """

    process_name = StringField(PROCESS_NAME)
    entry_name = StringField(ENTRY_NAME)
    unit_of_work_id = ObjectIdField(UNIT_OF_WORK_ID)

    @BaseDocument.key.getter
    def key(self):
        return self.process_name, self.entry_name

    @key.setter
    def key(self, value):
        if not isinstance(value, str):
            self.process_name = value[0]
            self.entry_name = value[1]
        else:
            self.process_name = value
            self.entry_name = None

    def __str__(self):
        return '%s::%s#%s' % (self.process_name, self.entry_name, self.unit_of_work_id)
