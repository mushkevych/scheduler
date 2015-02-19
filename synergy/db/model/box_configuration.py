__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import DictField, StringField

BOX_ID = 'box_id'
PROCESS_LIST = 'process_list'
PID = 'pid'
STATE = 'state'
STATE_ON = 'state_on'
STATE_OFF = 'state_off'


class BoxConfiguration(BaseDocument):
    """
    Class presents list of processes that are supposed to run on particular box.
    """
    box_id = StringField(BOX_ID)
    process_list = DictField(PROCESS_LIST)

    def _get_process_entry(self, process_name):
        if process_name not in self.process_list:
            self.process_list[process_name] = dict()
        return self.process_list[process_name]

    def set_process_state(self, process_name, value):
        if value not in [STATE_ON, STATE_OFF]:
            raise ValueError('incorrect state for process %r' % value)
        process = self._get_process_entry(process_name)
        process[STATE] = value

    def get_process_state(self, process_name):
        process = self._get_process_entry(process_name)
        return process.get(STATE)

    def set_process_pid(self, process_name, value):
        process = self._get_process_entry(process_name)
        process[PID] = value

    def get_process_pid(self, process_name):
        process = self._get_process_entry(process_name)
        return process.get(PID)
