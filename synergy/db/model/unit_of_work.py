__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, ObjectIdField, IntegerField, DictField, DateTimeField

TIMEPERIOD = 'timeperiod'
START_TIMEPERIOD = 'start_timeperiod'  # lower boundary (as Synergy date) of the period that needs to be processed
END_TIMEPERIOD = 'end_timeperiod'      # upper boundary (as Synergy date) of the period that needs to be processed
START_ID = 'start_obj_id'              # lower boundary (as DB _id) of the period that needs to be processed
END_ID = 'end_obj_id'                  # upper boundary (as DB _id) of the period that needs to be processed
STATE = 'state'
CREATED_AT = 'created_at'
SUBMITTED_AT = 'submitted_at'
STARTED_AT = 'started_at'
FINISHED_AT = 'finished_at'
NUMBER_OF_AGGREGATED_DOCUMENTS = 'number_of_aggregated_documents'
NUMBER_OF_PROCESSED_DOCUMENTS = 'number_of_processed_documents'
NUMBER_OF_RETRIES = 'number_of_retries'

PROCESS_NAME = 'process_name'          # process name of the aggregator/alarm/etc that processed the range
SOURCE = 'source'                      # defines source of data for the computation
SINK = 'sink'                          # defines sink where the aggregated data will be inserted
ARGUMENTS = 'arguments'                # task-level arguments that could supplement or override process-level ones

UNIT_OF_WORK_TYPE = 'unit_of_work_type'     # whether the unit_of_work is TYPE_MANAGED or TYPE_FREERUN
TYPE_MANAGED = 'type_managed'               # identifies UOW created by Abstract State Machine child for Managed Process
TYPE_FREERUN = 'type_freerun'               # identifies UOW created by FreerunStateMachine for ad-hock processing

# UOW was successfully processed by the worker
STATE_PROCESSED = 'state_processed'

# UOW was received by the worker and it started the processing
STATE_IN_PROGRESS = 'state_in_progress'

# UOW was instantiated and send to the worker
STATE_REQUESTED = 'state_requested'

# Job has been manually marked as SKIPPED via MX
# and so the associated UOW got cancelled
# or the life-support threshold has been crossed for failing UOW
STATE_CANCELED = 'state_canceled'

# UOW can get into STATE_INVALID if:
# a. related Job was marked for reprocessing via MX
# b. have failed with an exception at the worker level
# NOTICE: GarbageCollector changes STATE_INVALID -> STATE_REQUESTED during re-posting
STATE_INVALID = 'state_invalid'

# UOW was received by a worker,
# but no data was found to process
STATE_NOOP = 'state_noop'


class UnitOfWork(BaseDocument):
    """ Module represents persistent Model for atomic unit of work performed by the system.
    UnitOfWork Instances are stored in the <unit_of_work> collection """

    db_id = ObjectIdField('_id', null=True)
    process_name = StringField(PROCESS_NAME)
    timeperiod = StringField(TIMEPERIOD, null=True)
    start_timeperiod = StringField(START_TIMEPERIOD, null=True)
    end_timeperiod = StringField(END_TIMEPERIOD, null=True)
    start_id = ObjectIdField(START_ID)
    end_id = ObjectIdField(END_ID)
    source = StringField(SOURCE, null=True)
    sink = StringField(SINK, null=True)
    arguments = DictField(ARGUMENTS)
    state = StringField(STATE, choices=[STATE_INVALID, STATE_REQUESTED, STATE_IN_PROGRESS,
                                        STATE_PROCESSED, STATE_CANCELED, STATE_NOOP])
    created_at = DateTimeField(CREATED_AT)
    submitted_at = DateTimeField(SUBMITTED_AT)
    started_at = DateTimeField(STARTED_AT)
    finished_at = DateTimeField(FINISHED_AT)

    number_of_aggregated_documents = IntegerField(NUMBER_OF_AGGREGATED_DOCUMENTS)
    number_of_processed_documents = IntegerField(NUMBER_OF_PROCESSED_DOCUMENTS)
    number_of_retries = IntegerField(NUMBER_OF_RETRIES, default=0)
    unit_of_work_type = StringField(UNIT_OF_WORK_TYPE, choices=[TYPE_MANAGED, TYPE_FREERUN])

    @property
    def key(self):
        return self.process_name, self.timeperiod, self.start_id, self.end_id

    @property
    def is_active(self):
        return self.state in [STATE_REQUESTED, STATE_IN_PROGRESS, STATE_INVALID]

    @property
    def is_finished(self):
        return self.state in [STATE_PROCESSED, STATE_CANCELED, STATE_NOOP]

    @property
    def is_processed(self):
        return self.state == STATE_PROCESSED

    @property
    def is_noop(self):
        return self.state == STATE_NOOP

    @property
    def is_canceled(self):
        return self.state == STATE_CANCELED

    @property
    def is_invalid(self):
        return self.state == STATE_INVALID

    @property
    def is_requested(self):
        return self.state == STATE_REQUESTED

    @property
    def is_in_progress(self):
        return self.state == STATE_IN_PROGRESS
