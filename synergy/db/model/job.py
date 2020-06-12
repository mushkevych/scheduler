__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, ObjectIdField, ListField, IntegerField

# number of job events to be stored in Job.event_log, such as emission of the UOW
EVENT_LOG_MAX_SIZE = 128

# given Job was _not_ processed by aggregator because of multiple errors/missing data
# this state allows to mute current Job abd allow other timeperiods/Jobs to be processed
# only manual "re-processing" can re-run the skipped Job
STATE_SKIPPED = 'state_skipped'

# given Job was successfully processed by an aggregator
# no further processing for this Job is performed
STATE_PROCESSED = 'state_processed'

# no processing was performed for this Job
# no further processing for this Job is performed
STATE_NOOP = 'state_noop'

# Scheduler assumes that all timeperiod data is in the database, and asks an aggregator to run a "final" aggregation
# Job will be marked as STATE_PROCESSED afterwards if the processing succeed
STATE_FINAL_RUN = 'state_final_run'

# Aggregator is asked to perform a routine aggregation.
# Further state of the Job depends on the governing state machine:
# it could be either STATE_PROCESSED, STATE_IN_PROGRESS, STATE_NOOP, STATE_FINAL_RUN or STATE_SKIPPED
STATE_IN_PROGRESS = 'state_in_progress'

# Given timetable record serves as place-holder in the Tree
# TimeRecord can move to STATE_IN_PROGRESS
STATE_EMBRYO = 'state_embryo'


class Job(BaseDocument):
    """ class presents status for the time-period, and indicates whether data was process by particular process"""

    db_id = ObjectIdField(name='_id', null=True)
    process_name = StringField()
    timeperiod = StringField()
    state = StringField(choices=[STATE_IN_PROGRESS, STATE_PROCESSED, STATE_FINAL_RUN,
                                 STATE_EMBRYO, STATE_SKIPPED, STATE_NOOP])
    related_unit_of_work = ObjectIdField()

    event_log = ListField()
    number_of_failures = IntegerField(default=0)

    @classmethod
    def key_fields(cls):
        return cls.process_name.name, cls.timeperiod.name

    @property
    def is_active(self):
        return self.state in [STATE_FINAL_RUN, STATE_IN_PROGRESS, STATE_EMBRYO]

    @property
    def is_finished(self):
        return self.state in [STATE_PROCESSED, STATE_SKIPPED, STATE_NOOP]

    @property
    def is_processed(self):
        return self.state == STATE_PROCESSED

    @property
    def is_noop(self):
        return self.state == STATE_NOOP

    @property
    def is_skipped(self):
        return self.state == STATE_SKIPPED

    @property
    def is_embryo(self):
        return self.state == STATE_EMBRYO

    @property
    def is_in_progress(self):
        return self.state == STATE_IN_PROGRESS

    @property
    def is_final_run(self):
        return self.state == STATE_FINAL_RUN


TIMEPERIOD = Job.timeperiod.name
PROCESS_NAME = Job.process_name.name
STATE = Job.state.name
