__author__ = 'Bohdan Mushkevych'

from synergy.db.model.base_model import *

BOX_ID = 'box_id'
PROCESS_LIST = 'process_list'
PID = 'pid'
STATE = 'state'
STATE_ON = 'state_on'
STATE_OFF = 'state_off'


class BoxConfiguration(BaseModel):
    """
    Class presents list of processes that are supposed to run on particular box.
    """

    def __init__(self, document=None):
        super(BoxConfiguration, self).__init__(document)

    @property
    def box_id(self):
        return self.data[BOX_ID]

    @box_id.setter
    def box_id(self, value):
        self.data[BOX_ID] = value

    @property
    def process_list(self):
        return self._get_column_family(PROCESS_LIST)

    @process_list.setter
    def process_list(self, value):
        self.data[PROCESS_LIST] = value

    def _get_process_entry(self, process_name):
        family = self.process_list
        if process_name not in family:
            family[process_name] = dict()
        return family[process_name]

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
