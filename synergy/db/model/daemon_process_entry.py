__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, DictField, ListField

from synergy.scheduler.scheduler_constants import EXCHANGE_UTILS


PROCESS_NAME = 'process_name'
CLASSNAME = 'classname'
MQ_QUEUE = 'mq_queue'
MQ_EXCHANGE = 'mq_exchange'
MQ_ROUTING_KEY = 'mq_routing_key'
ARGUMENTS = 'arguments'
TOKEN = 'token'
LOG_FILENAME = 'log_filename'
LOG_TAG = 'log_tag'
PID_FILENAME = 'pid_filename'
PRESENT_ON_BOXES = 'present_on_boxes'     # list of boxes where this process is monitored by the Supervisor


class DaemonProcessEntry(BaseDocument):
    """ Non-persistent model. This class presents Process Context Entry record """

    process_name = StringField(PROCESS_NAME)
    classname = StringField(CLASSNAME)
    token = StringField(TOKEN)
    mq_queue = StringField(MQ_QUEUE)
    mq_exchange = StringField(MQ_EXCHANGE)
    mq_routing_key = StringField(MQ_ROUTING_KEY)
    arguments = DictField(ARGUMENTS)
    present_on_boxes = ListField(PRESENT_ON_BOXES, null=True)
    pid_filename = StringField(PID_FILENAME)
    log_filename = StringField(LOG_FILENAME)

    @BaseDocument.key.getter
    def key(self):
        return self.process_name

    @key.setter
    def key(self, value):
        """ :param value: name of the process """
        self.process_name = value


def daemon_context_entry(process_name,
                         classname,
                         token,
                         exchange=EXCHANGE_UTILS,
                         present_on_boxes=None,
                         arguments=None,
                         queue=None,
                         routing=None,
                         pid_file=None,
                         log_file=None):
    """ forms process context entry """
    _ROUTING_PREFIX = 'routing_'
    _QUEUE_PREFIX = 'queue_'
    _SUFFIX = '_daemon'

    if arguments is not None:
        assert isinstance(arguments, dict)

    process_entry = DaemonProcessEntry(
        process_name=process_name,
        classname=classname,
        token=token,
        mq_queue=queue if queue is not None else _QUEUE_PREFIX + token + _SUFFIX,
        mq_routing_key=routing if routing is not None else _ROUTING_PREFIX + token + _SUFFIX,
        mq_exchange=exchange,
        present_on_boxes=present_on_boxes,
        arguments=arguments if arguments is not None else dict(),
        log_filename=log_file if log_file is not None else token + _SUFFIX + '.log',
        pid_filename=pid_file if pid_file is not None else token + _SUFFIX + '.pid')
    return process_entry
