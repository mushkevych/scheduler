__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, ObjectIdField, ListField, IntegerField

MAX_NUMBER_OF_EVENTS = 128
TIMEPERIOD = 'timeperiod'
PROCESS_NAME = 'process_name'
STATE = 'state'
RELATED_UNIT_OF_WORK = 'related_unit_of_work'
NUMBER_OF_FAILURES = 'number_of_failures'

# contains list of last MAX_NUMBER_OF_EVENTS job events, such as emission of the UOW
EVENT_LOG = 'event_log'

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

    db_id = ObjectIdField('_id', null=True)
    process_name = StringField(PROCESS_NAME)
    timeperiod = StringField(TIMEPERIOD)
    state = StringField(STATE, choices=[STATE_IN_PROGRESS, STATE_PROCESSED, STATE_FINAL_RUN,
                                        STATE_EMBRYO, STATE_SKIPPED, STATE_NOOP])
    related_unit_of_work = ObjectIdField(RELATED_UNIT_OF_WORK)
    event_log = ListField(EVENT_LOG)
    number_of_failures = IntegerField(NUMBER_OF_FAILURES, default=0)

    @BaseDocument.key.getter
    def key(self):
        return self.process_name, self.timeperiod

    @key.setter
    def key(self, value):
        """ :param value: tuple (name of the process, timeperiod as string in Synergy Data format) """
        self.process_name = value[0]
        self.timeperiod = value[1]

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
