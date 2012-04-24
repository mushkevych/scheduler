"""
Created on 2011-02-08

@author: Bohdan Mushkevych
"""

class SchedulerConfigurationEntry(object):
    """
    Class presents single configuration entry for the scheduler. 
    """
    
    INTERVAL = 'interval_seconds'
    PROCESS_NAME = 'process_name'
    
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

    def get_document(self):
        return self.data