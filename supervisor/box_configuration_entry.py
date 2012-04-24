"""
Created on 2011-06-14

@author: Bohdan Mushkevych
"""

from model.abstract_model import AbstractModel

class BoxConfigurationEntry(AbstractModel):
    """
    Class presents list of processes that are supposed to run on particular box.
    """
    BOX_ID = 'box_id'
    PROCESS_LIST = 'process_list'

    PID = 'pid'
    STATE = 'state'
    
    STATE_ON = 'state_on'
    STATE_OFF = 'state_off'

    def __init__(self, document = None):
        super(BoxConfigurationEntry, self).__init__(document)

    def set_box_id(self, value):
        self.data[self.BOX_ID] = value

    def get_box_id(self):
        return self.data[self.BOX_ID]

    def set_process_list(self, value):
        self.data[self.PROCESS_LIST] = value

    def get_process_list(self):
        return self._get_column_family(self.PROCESS_LIST)

    def _get_process_entry(self, process_name):
        family = self.get_process_list()
        if process_name not in family:
            family[process_name] = dict()
        return family[process_name]

    def set_process_state(self, process_name, value):
        if value not in [self.STATE_ON, self.STATE_OFF]:
            raise ValueError('incorrect state for process %r' % value)
        process = self._get_process_entry(process_name)
        process[self.STATE] = value

    def get_process_state(self, process_name):
        process = self._get_process_entry(process_name)
        return process.get(self.STATE)

    def set_process_pid(self, process_name, value):
        process = self._get_process_entry(process_name)
        process[self.PID] = value

    def get_process_pid(self, process_name):
        process = self._get_process_entry(process_name)
        return process.get(self.PID)
