__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, ObjectIdField, ListField, IntegerField

MAX_NUMBER_OF_LOG_ENTRIES = 32
TIMEPERIOD = 'timeperiod'
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
STATE_EMBRYO = 'state_embryo'


class Job(BaseDocument):
    """ class presents status for the time-period, and indicates whether data was process by particular process"""

    db_id = ObjectIdField('_id', null=True)
    process_name = StringField(PROCESS_NAME)
    timeperiod = StringField(TIMEPERIOD)
    start_id = ObjectIdField(START_OBJ_ID)
    end_id = ObjectIdField(END_OBJ_ID)
    state = StringField(STATE,
                        choices=[STATE_IN_PROGRESS, STATE_PROCESSED, STATE_FINAL_RUN, STATE_EMBRYO, STATE_SKIPPED])
    related_unit_of_work = ObjectIdField(RELATED_UNIT_OF_WORK)
    log = ListField(HISTORIC_LOG)
    number_of_failures = IntegerField(NUMBER_OF_FAILURES, default=0)

    @property
    def key(self):
        return self.process_name, self.timeperiod

    @key.setter
    def key(self, value):
        """ :param value: tuple (name of the process, timeperiod as string in Synergy Data format) """
        self.process_name = value[0]
        self.timeperiod = value[1]
