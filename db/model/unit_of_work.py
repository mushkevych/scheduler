__author__ = 'Bohdan Mushkevych'

from db.model.base_model import BaseModel
from db.model.raw_data import *

START_TIMEPERIOD = 'start_timeperiod'  # lower boundary (as Synergy date) of the period that needs to be processed
END_TIMEPERIOD = 'end_timeperiod'      # upper boundary (as Synergy date) of the period that needs to be processed
START_OBJ_ID = 'start_obj_id'          # lower boundary (as MongoDB _id) of the period that needs to be processed
END_OBJ_ID = 'end_obj_id'              # upper boundary (as MongoDB _id) of the period that needs to be processed
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


class UnitOfWork(BaseModel):
    """ This class serves as a wrapper for the "units_of_work_collection" entry
    """

    def __init__(self, document=None):
        super(UnitOfWork, self).__init__(document)

    @property
    def timeperiod(self):
        return self.data[TIMEPERIOD]

    @timeperiod.setter
    def timeperiod(self, value):
        self.data[TIMEPERIOD] = value

    @property
    def start_timeperiod(self):
        return self.data.get(START_TIMEPERIOD)

    @start_timeperiod.setter
    def start_timeperiod(self, value):
        self.data[START_TIMEPERIOD] = value

    @property
    def end_timeperiod(self):
        return self.data.get(END_TIMEPERIOD)

    @end_timeperiod.setter
    def end_timeperiod(self, value):
        self.data[END_TIMEPERIOD] = value

    @property
    def start_id(self):
        return self.data[START_OBJ_ID]

    @start_id.setter
    def start_id(self, value):
        self.data[START_OBJ_ID] = value

    @property
    def end_id(self):
        return self.data[END_OBJ_ID]

    @end_id.setter
    def end_id(self, value):
        self.data[END_OBJ_ID] = value

    @property
    def source_collection(self):
        return self.data[SOURCE_COLLECTION]

    @source_collection.setter
    def source_collection(self, value):
        self.data[SOURCE_COLLECTION] = value

    @property
    def target_collection(self):
        return self.data[TARGET_COLLECTION]

    @target_collection.setter
    def target_collection(self, value):
        self.data[TARGET_COLLECTION] = value

    @property
    def state(self):
        return self.data[STATE]

    @state.setter
    def state(self, value):
        if not UnitOfWork.is_state_valid(value):
            raise ValueError('unit of work is in incorrect state')
        self.data[STATE] = value

    @classmethod
    def is_state_valid(cls, value):
        decision = True
        if value not in [STATE_INVALID, STATE_REQUESTED, STATE_IN_PROGRESS, STATE_PROCESSED, STATE_CANCELED]:
            decision = False
        return decision

    @property
    def created_at(self):
        return self.data.get(CREATED_AT)

    @created_at.setter
    def created_at(self, value):
        self.data[CREATED_AT] = value

    @property
    def started_at(self):
        return self.data.get(STARTED_AT)

    @started_at.setter
    def started_at(self, value):
        self.data[STARTED_AT] = value

    @property
    def finished_at(self):
        return self.data.get(FINISHED_AT)

    @finished_at.setter
    def finished_at(self, value):
        self.data[FINISHED_AT] = value

    @property
    def number_of_aggregated_documents(self):
        return self.data.get(NUMBER_OF_AGGREGATED_DOCUMENTS)

    @number_of_aggregated_documents.setter
    def number_of_aggregated_documents(self, value):
        self.data[NUMBER_OF_AGGREGATED_DOCUMENTS] = value

    @property
    def number_of_processed_documents(self):
        return self.data.get(NUMBER_OF_PROCESSED_DOCUMENTS)

    @number_of_processed_documents.setter
    def number_of_processed_documents(self, value):
        self.data[NUMBER_OF_PROCESSED_DOCUMENTS] = value

    @property
    def number_of_retries(self):
        return self.data.get(NUMBER_OF_RETRIES)

    @number_of_retries.setter
    def number_of_retries(self, value):
        self.data[NUMBER_OF_RETRIES] = value

    @property
    def process_name(self):
        return self.data[PROCESS_NAME]

    @process_name.setter
    def process_name(self, value):
        self.data[PROCESS_NAME] = value

    @property
    def processed_log(self):
        return self.data.get(PROCESSED_LOG)

    @processed_log.setter
    def processed_log(self, value):
        self.data[PROCESSED_LOG] = value
