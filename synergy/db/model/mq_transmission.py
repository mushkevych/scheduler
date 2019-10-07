__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, ObjectIdField

PROCESS_NAME = 'process_name'           # name of the process to handle the schedulables
ENTRY_NAME = 'entry_name'               # name of the schedulable, if applicable
RECORD_DB_ID = 'record_db_id'           # associated with either UOW.db_id or Job.db_id


class MqTransmission(BaseDocument):
    """ Non-persistent model. Instance of this class presents either:
     - single request from Synergy Scheduler to any worker
     - response/report from the worker to the Synergy Scheduler """

    process_name = StringField(PROCESS_NAME)
    entry_name = StringField(ENTRY_NAME, null=True)
    record_db_id = ObjectIdField(RECORD_DB_ID)

    @BaseDocument.key.getter
    def key(self):
        return self.process_name, self.entry_name

    @key.setter
    def key(self, value):
        if isinstance(value, (list, tuple)):
            self.process_name = value[0]
            self.entry_name = value[1]
        else:
            self.process_name = value
            self.entry_name = None

    def __str__(self):
        return f'{self.process_name}::{self.entry_name}#{self.record_db_id}'
