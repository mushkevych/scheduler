"""
Created on 2011-02-07

@author: Bohdan Mushkevych
"""
class UnitsOfWorkCollection(object):
    """
    This class serves as a wrapper for the "units_of_work_collection" entry
    
    """
    TIMESTAMP = 'timestamp'             # period that needs to be processed. presented as Synergy Timestamp
    START_TIMESTAMP = 'start_timestamp' # lower boundary (as Synergy date) of the period that needs to be processed
    END_TIMESTAMP = 'end_timestamp'     # upper boundary (as Synergy date) of the period that needs to be processed
    START_OBJ_ID = 'start_obj_id'       # lower boundary (as MongoDB _id) of the period that needs to be processed
    END_OBJ_ID = 'end_obj_id'           # upper boundary (as MongoDB _id) of the period that needs to be processed
    STATE = 'state'
    CREATED_AT = 'created_at'
    STARTED_AT = 'started_at'
    FINISHED_AT = 'finished_at'
    NUMBER_OF_AGGREGATED_DOCUMENTS = 'number_of_aggregated_documents'
    NUMBER_OF_PROCESSED_DOCUMENTS = 'number_of_processed_documents'
    NUMBER_OF_RETRIES = 'number_of_retries'
    
    # process name of the aggregator/alarm/etc that processed the range
    PROCESS_NAME = 'process_name' 
    # source_collection defines source data for the computation
    SOURCE_COLLECTION = 'source_collection'
    # target_collection defines collection, where aggregated data will be inserted
    TARGET_COLLECTION = 'target_collection'
    # log contains list of processed files or other artifacts
    PROCESSED_LOG = 'processed_log'
    # Name of processed file
    FILE_NAME = 'file_name'
    # MD5 tag for the hash of the file
    MD5 = 'md5'

    STATE_PROCESSED = 'state_processed'
    STATE_IN_PROGRESS = 'state_in_progress'
    STATE_REQUESTED = 'state_requested'
    STATE_CANCELED = 'state_canceled'
    STATE_INVALID = 'state_invalid'
    
    def __init__(self, document = None):
        if document is None:
            self.data = dict()
        else:
            self.data = document
    
    def set_timestamp(self, value):
        self.data[self.TIMESTAMP] = value

    def get_timestamp(self):
        return self.data[self.TIMESTAMP]

    def set_start_timestamp(self, value):
        self.data[self.START_TIMESTAMP] = value

    def get_start_timestamp(self):
        return self.data.get(self.START_TIMESTAMP)

    def set_end_timestamp(self, value):
        self.data[self.END_TIMESTAMP] = value

    def get_end_timestamp(self):
        return self.data.get(self.END_TIMESTAMP)

    def set_start_id(self, value):
        self.data[self.START_OBJ_ID] = value
        
    def get_start_id(self):
        return self.data[self.START_OBJ_ID]

    def set_end_id(self, value):
        self.data[self.END_OBJ_ID] = value

    def get_end_id(self):
        return self.data[self.END_OBJ_ID]

    def set_source_collection(self, value):
        self.data[self.SOURCE_COLLECTION] = value

    def get_source_collection(self):
        return self.data[self.SOURCE_COLLECTION]

    def set_target_collection(self, value):
        self.data[self.TARGET_COLLECTION] = value

    def get_target_collection(self):
        return self.data[self.TARGET_COLLECTION]

    def set_state(self, value):
        if not UnitsOfWorkCollection.is_state_valid(value):
            raise ValueError('unit of work is in incorrect state')
        self.data[self.STATE] = value

    def get_state(self):
        return self.data[self.STATE]
    
    @classmethod
    def is_state_valid(cls, value):
        decision = True
        if value not in [cls.STATE_INVALID,
                         cls.STATE_REQUESTED,
                         cls.STATE_IN_PROGRESS,
                         cls.STATE_PROCESSED,
                         cls.STATE_CANCELED]:
            decision = False
        return decision
    
    def set_created_at(self, value):
        self.data[self.CREATED_AT] = value

    def get_created_at(self):
        return self.data.get(self.CREATED_AT)

    def set_started_at(self, value):
        self.data[self.STARTED_AT] = value

    def get_started_at(self):
        return self.data.get(self.STARTED_AT)

    def set_finished_at(self, value):
        self.data[self.FINISHED_AT] = value

    def get_finished_at(self):
        return self.data.get(self.FINISHED_AT)

    def set_number_of_aggregated_documents(self, value):
        self.data[self.NUMBER_OF_AGGREGATED_DOCUMENTS] = value

    def get_number_of_aggregated_documents(self):
        return self.data.get(self.NUMBER_OF_AGGREGATED_DOCUMENTS)

    def set_number_of_processed_documents(self, value):
        self.data[self.NUMBER_OF_PROCESSED_DOCUMENTS] = value

    def get_number_of_processed_documents(self):
        return self.data.get(self.NUMBER_OF_PROCESSED_DOCUMENTS)

    def set_number_of_retries(self, value):
        self.data[self.NUMBER_OF_RETRIES] = value

    def get_number_of_retries(self):
        return self.data.get(self.NUMBER_OF_RETRIES)

    def set_process_name(self, value):
        self.data[self.PROCESS_NAME] = value

    def get_process_name(self):
        return self.data[self.PROCESS_NAME]
    
    def set_processed_log(self, value):
        self.data[self.PROCESSED_LOG] = value

    def get_processed_log(self):
        return self.data.get(self.PROCESSED_LOG)

    def get_document(self):
        return self.data