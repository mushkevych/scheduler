__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, ObjectIdField, IntegerField, DictField, DateTimeField
from synergy.scheduler.scheduler_constants import TYPE_FREERUN, TYPE_MANAGED
from synergy.db.model.base_model import TIMEPERIOD

START_TIMEPERIOD = 'start_timeperiod'  # lower boundary (as Synergy date) of the period that needs to be processed
END_TIMEPERIOD = 'end_timeperiod'      # upper boundary (as Synergy date) of the period that needs to be processed
START_OBJ_ID = 'start_obj_id'          # lower boundary (as DB _id) of the period that needs to be processed
END_OBJ_ID = 'end_obj_id'              # upper boundary (as DB _id) of the period that needs to be processed
STATE = 'state'
CREATED_AT = 'created_at'
STARTED_AT = 'started_at'
FINISHED_AT = 'finished_at'
NUMBER_OF_AGGREGATED_DOCUMENTS = 'number_of_aggregated_documents'
NUMBER_OF_PROCESSED_DOCUMENTS = 'number_of_processed_documents'
NUMBER_OF_RETRIES = 'number_of_retries'

PROCESS_NAME = 'process_name'          # process name of the aggregator/alarm/etc that processed the range
SOURCE = 'source'                      # defines source of data for the computation
SINK = 'sink'                          # defines sink where the aggregated data will be inserted
PROCESSED_LOG = 'processed_log'        # log contains list of processed files or other artifacts
FILE_NAME = 'file_name'                # Name of processed file
MD5 = 'md5'                            # MD5 tag for the hash of the file
ARGUMENTS = 'arguments'                # task-level arguments that could supplement or override process-level ones
UNIT_OF_WORK_TYPE = 'unit_of_work_type'  # whether the unit_of_work is TYPE_MANAGED or TYPE_FREERUN

STATE_PROCESSED = 'state_processed'
STATE_IN_PROGRESS = 'state_in_progress'
STATE_REQUESTED = 'state_requested'
STATE_CANCELED = 'state_canceled'
STATE_INVALID = 'state_invalid'


class UnitOfWork(BaseDocument):
    """ Module represents persistent Model for atomic unit of work performed by the system.
    UnitOfWork Instances are stored in the <unit_of_work> collection """

    db_id = ObjectIdField('_id', null=True)
    process_name = StringField(PROCESS_NAME)
    timeperiod = StringField(TIMEPERIOD)
    start_timeperiod = StringField(START_TIMEPERIOD)
    end_timeperiod = StringField(END_TIMEPERIOD)
    start_id = ObjectIdField(START_OBJ_ID)
    end_id = ObjectIdField(END_OBJ_ID)
    source = StringField(SOURCE)
    sink = StringField(SINK)
    arguments = DictField(ARGUMENTS)
    state = StringField(STATE,
                        choices=[STATE_INVALID, STATE_REQUESTED, STATE_IN_PROGRESS, STATE_PROCESSED, STATE_CANCELED])
    created_at = DateTimeField(CREATED_AT)
    started_at = DateTimeField(STARTED_AT)
    finished_at = DateTimeField(FINISHED_AT)

    number_of_aggregated_documents = IntegerField(NUMBER_OF_AGGREGATED_DOCUMENTS)
    number_of_processed_documents = IntegerField(NUMBER_OF_PROCESSED_DOCUMENTS)
    number_of_retries = IntegerField(NUMBER_OF_RETRIES)
    processed_log = DictField(PROCESSED_LOG)
    unit_of_work_type = StringField(UNIT_OF_WORK_TYPE, choices=[TYPE_MANAGED, TYPE_FREERUN])
