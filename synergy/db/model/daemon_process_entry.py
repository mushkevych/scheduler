__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, DictField, ListField

from synergy.scheduler.scheduler_constants import EXCHANGE_UTILS


class DaemonProcessEntry(BaseDocument):
    """ Non-persistent model. This class presents Process Context Entry record """

    process_name = StringField()
    classname = StringField()
    token = StringField()
    mq_queue = StringField()
    mq_exchange = StringField()
    mq_routing_key = StringField()
    arguments = DictField()
    present_on_boxes = ListField(null=True)  # list of boxes where this process is monitored by the Supervisor
    pid_filename = StringField()
    log_filename = StringField()

    @classmethod
    def key_fields(cls):
        return cls.process_name.name


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
