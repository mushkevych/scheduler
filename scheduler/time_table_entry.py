"""
Created on 2011-03-18

@author: Bohdan Mushkevych
"""
MAX_NUMBER_OF_LOG_ENTRIES = 32

class TimeTableEntry(object):
    """
    This class presents status for the time-period, and indicates whether data was process by particular process
    
    """
    TIMESTAMP = 'timestamp'            # timestamp of the period that needs to be processed
    PROCESS_NAME = 'process_name'
    START_OBJ_ID = 'start_obj_id'
    END_OBJ_ID = 'end_obj_id'
    STATE = 'state'
    RELATED_UNIT_OF_WORK = 'related_unit_of_work'
    NUMBER_OF_FAILURES = 'number_of_failures'

    # contains list of MAX_NUMBER_OF_LOG_ENTRIES last log messages
    HISTORIC_LOG = 'historic_log'

    # given timeperiod was _not_ processed by aggregator because of multiple errors/missing data
    # time period was muted to allow other timeperiod be processed
    # only manual "re-processing" can re-run the timeperiod
    STATE_SKIPPED = 'state_skipped'

    # means that given timeperiod was successfully processed by aggregator
    # no further processing for timeperiod is performed 
    STATE_PROCESSED = 'state_processed' 
    
    # Scheduler assumes that all timeperiod data is in the database, and asks aggregator to run a "final" aggregation
    # TimeRecord will be marked as STATE_PROCESSED afterwards
    STATE_FINAL_RUN = 'state_final_run'      
    
    # Aggregator is asked to perform routine aggregation.
    # TimeRecord will _not_ move to STATE_PROCESSED afterwards
    STATE_IN_PROGRESS = 'state_in_progress'   
   
    # Given timetable record serves as place-holder in the Tree
    # TimeRecord can move to STATE_IN_PROGRESS
    STATE_EMBRYO  = 'state_embryo'

    def __init__(self, document = None):
        if document is None:
            self.data = dict()
        else:
            self.data = document
    
    def set_process_name(self, value):
        self.data[self.PROCESS_NAME] = value

    def get_process_name(self):
        return self.data[self.PROCESS_NAME]
    
    def set_timestamp(self, value):
        self.data[self.TIMESTAMP] = value
        
    def get_timestamp(self):
        return self.data[self.TIMESTAMP]

    def set_start_id(self, value):
        self.data[self.START_OBJ_ID] = value
        
    def get_start_id(self):
        return self.data[self.START_OBJ_ID]

    def set_end_id(self, value):
        self.data[self.END_OBJ_ID] = value

    def get_end_id(self):
        return self.data[self.END_OBJ_ID]

    def set_state(self, value):
        if not TimeTableEntry.is_state_valid(value):
            raise ValueError('unit of work is in incorrect state')
        self.data[self.STATE] = value

    def get_state(self):
        return self.data[self.STATE]
    
    @classmethod
    def is_state_valid(cls, value):
        decision = True
        if value not in [cls.STATE_IN_PROGRESS, cls.STATE_PROCESSED,
                         cls.STATE_FINAL_RUN, cls.STATE_EMBRYO, cls.STATE_SKIPPED]:
            decision = False
        return decision
    
    def set_related_unit_of_work(self, value):
        self.data[self.RELATED_UNIT_OF_WORK] = value

    def get_related_unit_of_work(self):
        return self.data.get(self.RELATED_UNIT_OF_WORK)

    def set_log(self, value):
        self.data[self.HISTORIC_LOG] = value

    def get_log(self):
        if self.HISTORIC_LOG not in self.data:
            self.data[self.HISTORIC_LOG] = []
        return self.data[self.HISTORIC_LOG]

    def get_number_of_failures(self):
        if self.NUMBER_OF_FAILURES not in self.data:
            self.data[self.NUMBER_OF_FAILURES] = 0
        return self.data[self.NUMBER_OF_FAILURES]

    def set_number_of_failures(self, value):
        self.data[self.NUMBER_OF_FAILURES] = value

    def get_document(self):
        return self.data