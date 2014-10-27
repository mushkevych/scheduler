__author__ = 'Bohdan Mushkevych'

from synergy.db.model.base_model import *

PROCESS_NAME = 'process_name'           # name of the process to handle the schedulables
ENTRY_NAME = 'entry_name'               # name of the schedulable, if applicable
UNIT_OF_WORK_ID = 'unit_of_work_id'     # associated Unit Of Work, if applicable


class WorkerMqRequest(BaseModel):
    """ Non-persistent model. Instance of this class presents single request from Synergy Scheduler to any worker """

    def __init__(self, document=None):
        super(WorkerMqRequest, self).__init__(document)

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
    def unit_of_work_id(self):
        return self.data[UNIT_OF_WORK_ID]

    @unit_of_work_id.setter
    def unit_of_work_id(self, value):
        if not isinstance(value, str):
            value = str(value)
        self.data[UNIT_OF_WORK_ID] = value
