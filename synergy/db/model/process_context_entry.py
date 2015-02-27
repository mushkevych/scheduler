__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, BooleanField, DictField
from synergy.scheduler.scheduler_constants import TYPE_MANAGED, TYPE_FREERUN, TYPE_GARBAGE_COLLECTOR

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
LOG_FILENAME = 'log_filename'
LOG_TAG = 'log_tag'
PID_FILENAME = 'pid_filename'
RUN_ON_ACTIVE_TIMEPERIOD = 'run_on_active_timeperiod'


class ProcessContextEntry(BaseDocument):
    """ Non-persistent model. This class presents Process Context Entry record """

    process_name = StringField(PROCESS_NAME)
    classname = StringField(CLASSNAME)
    token = StringField(TOKEN)
    source = StringField(SOURCE)
    sink = StringField(SINK)
    mq_queue = StringField(MQ_QUEUE)
    mq_exchange = StringField(MQ_EXCHANGE)
    mq_routing_key = StringField(MQ_ROUTING_KEY)
    time_qualifier = StringField(TIME_QUALIFIER)
    arguments = DictField(ARGUMENTS)
    process_type = StringField(PROCESS_TYPE, choices=[TYPE_MANAGED, TYPE_FREERUN, TYPE_GARBAGE_COLLECTOR])
    run_on_active_timeperiod = BooleanField(RUN_ON_ACTIVE_TIMEPERIOD)
    pid_filename = StringField(PID_FILENAME)
    log_filename = StringField(LOG_FILENAME)

    @BaseDocument.key.getter
    def key(self):
        return self.process_name

    @key.setter
    def key(self, value):
        """ :param value: name of the process """
        self.process_name = value

    @property
    def log_tag(self):
        return str(self.token) + str(self.time_qualifier)


def _process_context_entry(process_name,
                           classname,
                           token,
                           time_qualifier,
                           exchange,
                           arguments=None,
                           queue=None,
                           routing=None,
                           process_type=None,
                           source=None,
                           sink=None,
                           pid_file=None,
                           log_file=None,
                           run_on_active_timeperiod=False):
    """ forms process context entry """
    _ROUTING_PREFIX = 'routing_'
    _QUEUE_PREFIX = 'queue_'

    if queue is None:
        queue = _QUEUE_PREFIX + token + time_qualifier
    if routing is None:
        routing = _ROUTING_PREFIX + token + time_qualifier
    if pid_file is None:
        pid_file = token + time_qualifier + '.pid'
    if log_file is None:
        log_file = token + time_qualifier + '.log'
    if arguments is None:
        arguments = dict()
    else:
        assert isinstance(arguments, dict)

    process_entry = ProcessContextEntry(
        process_name=process_name,
        classname=classname,
        token=token,
        source=source,
        sink=sink,
        mq_queue=queue,
        mq_routing_key=routing,
        mq_exchange=exchange,
        arguments=arguments,
        time_qualifier=time_qualifier,
        process_type=process_type,
        log_filename=log_file,
        pid_filename=pid_file,
        run_on_active_timeperiod=run_on_active_timeperiod)
    return process_entry
