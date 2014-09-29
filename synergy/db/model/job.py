__author__ = 'Bohdan Mushkevych'

from synergy.db.model.base_model import *

MAX_NUMBER_OF_LOG_ENTRIES = 32
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


class Job(BaseModel):
    """ class presents status for the time-period, and indicates whether data was process by particular process"""

    def __init__(self, document=None):
        super(Job, self).__init__(document)

    @property
    def key(self):
        return self.data[PROCESS_NAME], self.data[TIMEPERIOD]

    @key.setter
    def key(self, value):
        """
        @param value: tuple - value[0] - name of the process
        value[1] - timeperiod as string in Synergy Data format
        """
        self.data[PROCESS_NAME] = value[0]
        self.data[TIMEPERIOD] = value[1]

    @property
    def process_name(self):
        return self.data[PROCESS_NAME]

    @process_name.setter
    def process_name(self, value):
        self.data[PROCESS_NAME] = value

    @property
    def timeperiod(self):
        return self.data[TIMEPERIOD]

    @timeperiod.setter
    def timeperiod(self, value):
        self.data[TIMEPERIOD] = value

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
    def state(self):
        return self.data[STATE]

    @state.setter
    def state(self, value):
        if not Job.is_state_valid(value):
            raise ValueError('unit of work is in incorrect state')
        self.data[STATE] = value

    @classmethod
    def is_state_valid(cls, value):
        decision = True
        if value not in [STATE_IN_PROGRESS, STATE_PROCESSED, STATE_FINAL_RUN, STATE_EMBRYO, STATE_SKIPPED]:
            decision = False
        return decision

    @property
    def related_unit_of_work(self):
        return self.data.get(RELATED_UNIT_OF_WORK)

    @related_unit_of_work.setter
    def related_unit_of_work(self, value):
        self.data[RELATED_UNIT_OF_WORK] = value

    @property
    def log(self):
        if HISTORIC_LOG not in self.data:
            self.data[HISTORIC_LOG] = []
        return self.data[HISTORIC_LOG]

    @log.setter
    def log(self, value):
        self.data[HISTORIC_LOG] = value

    @property
    def number_of_failures(self):
        if NUMBER_OF_FAILURES not in self.data:
            self.data[NUMBER_OF_FAILURES] = 0
        return self.data[NUMBER_OF_FAILURES]

    @number_of_failures.setter
    def number_of_failures(self, value):
        self.data[NUMBER_OF_FAILURES] = value
