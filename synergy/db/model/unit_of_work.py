__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, ObjectIdField, IntegerField, DictField, DateTimeField


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

    db_id = ObjectIdField(name='_id', null=True)
    process_name = StringField()
    timeperiod = StringField(null=True)
    start_timeperiod = StringField(null=True)   # [synergy date] lower boundary of the period that needs to be processed
    end_timeperiod = StringField(null=True)     # [synergy date] upper boundary of the period that needs to be processed
    start_id = ObjectIdField(name='start_obj_id')   # [DB _id] lower boundary of the period that needs to be processed
    end_id = ObjectIdField(name='end_obj_id')       # [DB _id] upper boundary of the period that needs to be processed
    source = StringField(null=True)     # defines source of data for the computation
    sink = StringField(null=True)       # defines sink where the aggregated data will be saved
    arguments = DictField()             # task-level arguments that could supplement or override process-level ones
    state = StringField(choices=[STATE_INVALID, STATE_REQUESTED, STATE_IN_PROGRESS,
                                 STATE_PROCESSED, STATE_CANCELED, STATE_NOOP])
    created_at = DateTimeField()
    submitted_at = DateTimeField()
    started_at = DateTimeField()
    finished_at = DateTimeField()

    number_of_aggregated_documents = IntegerField()
    number_of_processed_documents = IntegerField()
    number_of_retries = IntegerField(default=0)
    unit_of_work_type = StringField(choices=[TYPE_MANAGED, TYPE_FREERUN])

    @classmethod
    def key_fields(cls):
        return (cls.process_name.name,
                cls.timeperiod.name,
                cls.start_id.name,
                cls.end_id.name)

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


PROCESS_NAME = UnitOfWork.process_name.name
TIMEPERIOD = UnitOfWork.timeperiod.name
START_TIMEPERIOD = UnitOfWork.start_timeperiod.name
END_TIMEPERIOD = UnitOfWork.end_timeperiod.name
START_ID = UnitOfWork.start_id.name
END_ID = UnitOfWork.end_id.name
STATE = UnitOfWork.state.name
UNIT_OF_WORK_TYPE = UnitOfWork.unit_of_work_type.name
