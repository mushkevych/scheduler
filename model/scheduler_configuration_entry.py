"""
Created on 2011-02-08

@author: Bohdan Mushkevych
"""

class SchedulerConfigurationEntry(object):
    """
    Class presents single configuration entry for the scheduler. 
    """
    
    PROCESS_NAME = 'process_name'
    STATE = 'state'
    INTERVAL = 'interval_seconds'

    STATE_ON = 'state_on'
    STATE_OFF = 'state_off'

    def __init__(self, document = None):
        if document is None:
            self.data = dict()
        else:
            self.data = document
    
    def set_interval(self, value):
        self.data[self.INTERVAL] = value

    def get_interval(self):
        return self.data[self.INTERVAL]

    def set_process_name(self, value):
        self.data[self.PROCESS_NAME] = value

    def get_process_name(self):
        return self.data[self.PROCESS_NAME]

    def set_process_state(self, value):
        if value not in [self.STATE_ON, self.STATE_OFF]:
            raise ValueError('incorrect state for process %r' % value)
        self.data[self.STATE] = value

    def get_process_state(self):
        return self.data[self.STATE]

    def get_document(self):
        return self.data