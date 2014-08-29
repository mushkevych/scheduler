__author__ = 'Bohdan Mushkevych'

from db.model.base_model import *

PROCESS_NAME = 'process_name'
CLASSNAME = 'classname'
SOURCE = 'source'
SINK = 'sink'
MQ_QUEUE = 'mq_queue'
MQ_EXCHANGE = 'mq_exchange'
MQ_ROUTING_KEY = 'mq_routing_key'
TIME_QUALIFIER = 'time_qualifier'
ARGUMENTS = 'arguments'
TOKEN = 'token'
PROCESS_TYPE = 'process_type'
PIPELINE_NAME = 'pipeline_name'
LOG_FILENAME = 'log_filename'
LOG_TAG = 'log_tag'
PID_FILENAME = 'pid_filename'


class ProcessContextEntry(BaseModel):
    """ This class presents Process Context Entry record """

    def __init__(self, document=None):
        super(ProcessContextEntry, self).__init__(document)

    @BaseModel.key.getter
    def key(self):
        return self.data[PROCESS_NAME]

    @key.setter
    def key(self, value):
        """
        @param value: name of the process
        """
        self.data[PROCESS_NAME] = value

    @property
    def process_name(self):
        return self.data[PROCESS_NAME]

    @process_name.setter
    def process_name(self, value):
        self.data[PROCESS_NAME] = value

    @property
    def classname(self):
        return self.data[CLASSNAME]

    @classname.setter
    def classname(self, value):
        self.data[CLASSNAME] = value

    @property
    def token(self):
        return self.data[TOKEN]

    @token.setter
    def token(self, value):
        self.data[TOKEN] = value

    @property
    def source(self):
        return self.data[SOURCE]

    @source.setter
    def source(self, value):
        self.data[SOURCE] = value

    @property
    def sink(self):
        return self.data[SINK]

    @sink.setter
    def sink(self, value):
        self.data[SINK] = value

    @property
    def mq_queue(self):
        return self.data[MQ_QUEUE]

    @mq_queue.setter
    def mq_queue(self, value):
        self.data[MQ_QUEUE] = value

    @property
    def mq_exchange(self):
        return self.data[MQ_EXCHANGE]

    @mq_exchange.setter
    def mq_exchange(self, value):
        self.data[MQ_EXCHANGE] = value

    @property
    def mq_routing_key(self):
        return self.data[MQ_ROUTING_KEY]

    @mq_routing_key.setter
    def mq_routing_key(self, value):
        self.data[MQ_ROUTING_KEY] = value

    @property
    def time_qualifier(self):
        return self.data[TIME_QUALIFIER]

    @time_qualifier.setter
    def time_qualifier(self, value):
        self.data[TIME_QUALIFIER] = value

    @property
    def arguments(self):
        return self.data[ARGUMENTS]

    @arguments.setter
    def arguments(self, value):
        self.data[ARGUMENTS] = value

    @property
    def process_type(self):
        return self.data[PROCESS_TYPE]

    @process_type.setter
    def process_type(self, value):
        self.data[PROCESS_TYPE] = value

    @property
    def state_machine_name(self):
        return self.data[PIPELINE_NAME]

    @state_machine_name.setter
    def state_machine_name(self, value):
        self.data[PIPELINE_NAME] = value

    @property
    def log_tag(self):
        return self.token + self.time_qualifier

    @property
    def log_filename(self):
        return self.data[LOG_FILENAME]

    @log_filename.setter
    def log_filename(self, value):
        self.data[LOG_FILENAME] = value

    @property
    def pid_filename(self):
        return self.data[PID_FILENAME]

    @pid_filename.setter
    def pid_filename(self, value):
        self.data[PID_FILENAME] = value
