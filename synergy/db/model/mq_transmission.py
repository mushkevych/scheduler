__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, ObjectIdField


class MqTransmission(BaseDocument):
    """ Non-persistent model. Instance of this class presents either:
     - single request from Synergy Scheduler to any worker
     - response/report from the worker to the Synergy Scheduler """

    process_name = StringField()            # name of the process to handle the schedulables
    entry_name = StringField(null=True)     # name of the schedulable, if applicable
    record_db_id = ObjectIdField()          # associated with either UOW.db_id or Job.db_id

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
