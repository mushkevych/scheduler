__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, BooleanField, DictField

from synergy.scheduler.scheduler_constants import TYPE_MANAGED, TYPE_FREERUN, TYPE_GARBAGE_COLLECTOR, EXCHANGE_UTILS, \
    TYPE_DAEMON


PROCESS_NAME = 'process_name'
CLASSNAME = 'classname'
MQ_QUEUE = 'mq_queue'
MQ_EXCHANGE = 'mq_exchange'
MQ_ROUTING_KEY = 'mq_routing_key'
ARGUMENTS = 'arguments'
TOKEN = 'token'
PROCESS_TYPE = 'process_type'
LOG_FILENAME = 'log_filename'
LOG_TAG = 'log_tag'
PID_FILENAME = 'pid_filename'
RUN_ON_ACTIVE_TIMEPERIOD = 'run_on_active_timeperiod'


class DaemonProcessEntry(BaseDocument):
    """ Non-persistent model. This class presents Process Context Entry record """

    process_name = StringField(PROCESS_NAME)
    classname = StringField(CLASSNAME)
    token = StringField(TOKEN)
    mq_queue = StringField(MQ_QUEUE)
    mq_exchange = StringField(MQ_EXCHANGE)
    mq_routing_key = StringField(MQ_ROUTING_KEY)
    arguments = DictField(ARGUMENTS)
    process_type = StringField(PROCESS_TYPE, choices=[TYPE_MANAGED, TYPE_FREERUN, TYPE_DAEMON, TYPE_GARBAGE_COLLECTOR])
    run_on_active_timeperiod = BooleanField(RUN_ON_ACTIVE_TIMEPERIOD)
    pid_filename = StringField(PID_FILENAME)
    log_filename = StringField(LOG_FILENAME)

    @BaseDocument.key.getter
    def key(self):
        return self.process_name

    @BaseDocument.key.setter
    def key(self, value):
        """ :param value: name of the process """
        self.process_name = value

    @property
    def log_tag(self):
        return str(self.token)


def deamon_context_entry(process_name,
                         classname,
                         token,
                         exchange=EXCHANGE_UTILS,
                         arguments=None,
                         queue=None,
                         routing=None,
                         process_type=TYPE_DAEMON,
                         pid_file=None,
                         log_file=None,
                         run_on_active_timeperiod=False):
    """ forms process context entry """
    _ROUTING_PREFIX = 'routing_'
    _QUEUE_PREFIX = 'queue_'
    _SUFFIX = '_daemon'

    if queue is None:
        queue = _QUEUE_PREFIX + token + _SUFFIX
    if routing is None:
        routing = _ROUTING_PREFIX + token + _SUFFIX
    if pid_file is None:
        pid_file = token + _SUFFIX + '.pid'
    if log_file is None:
        log_file = token + _SUFFIX + '.log'
    if arguments is None:
        arguments = dict()
    else:
        assert isinstance(arguments, dict)

    process_entry = DaemonProcessEntry(
        process_name=process_name,
        classname=classname,
        token=token,
        mq_queue=queue,
        mq_routing_key=routing,
        mq_exchange=exchange,
        arguments=arguments,
        process_type=process_type,
        log_filename=log_file,
        pid_filename=pid_file,
        run_on_active_timeperiod=run_on_active_timeperiod)
    return process_entry
