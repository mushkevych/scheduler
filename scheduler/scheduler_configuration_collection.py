"""
Created on 2011-02-08

@author: Bohdan Mushkevych
"""

class SchedulerConfigurationCollection(object):
    """
    Class presents single configuration entry for the scheduler. 
    """
    
    ENTRY_NAME = 'entry_name'
    INTERVAL = 'interval_seconds'
    TYPE = 'type'
    PROCESS_NAME = 'process_name'
    
    TYPE_ALERT = 'type_alert'
    TYPE_HORIZONTAL_AGGREGATOR = 'type_horizontal_aggregator' 
    TYPE_VERTICAL_AGGREGATOR = 'type_vertical_aggregator'
    TYPE_GARBAGE_COLLECTOR = 'type_garbage_collector'

    def __init__(self, document = None):
        if document is None:
            self.data = dict()
        else:
            self.data = document
    
    def set_entry_name(self, value):
        self.data[self.ENTRY_NAME] = value
        
    def get_entry_name(self):
        return self.data[self.ENTRY_NAME]

    def set_interval(self, value):
        self.data[self.INTERVAL] = value

    def get_interval(self):
        return self.data[self.INTERVAL]

    def set_type(self, value):
        if value not in [self.TYPE_ALERT,
                         self.TYPE_HORIZONTAL_AGGREGATOR,
                         self.TYPE_VERTICAL_AGGREGATOR,
                         self.TYPE_GARBAGE_COLLECTOR]:
            raise ValueError('incorrect type of scheduler event')
        self.data[self.TYPE] = value

    def get_type(self):
        return self.data[self.TYPE]
    
    def set_process_name(self, value):
        self.data[self.PROCESS_NAME] = value

    def get_process_name(self):
        return self.data[self.PROCESS_NAME]

    def get_document(self):
        return self.data