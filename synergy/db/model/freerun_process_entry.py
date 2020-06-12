__author__ = 'Bohdan Mushkevych'

from synergy.scheduler.scheduler_constants import STATE_MACHINE_FREERUN, EXCHANGE_FREERUN_WORKER
from synergy.db.model.daemon_process_entry import DaemonProcessEntry
from odm.fields import StringField, ListField, ObjectIdField, BooleanField

# contains list of last EVENT_LOG_MAX_SIZE job events, such as emission of the UOW
MAX_NUMBER_OF_EVENTS = 128


def split_schedulable_name(name):
    return name.split('::', 1)


def build_schedulable_name(prefix, suffix):
    return '{0}::{1}'.format(prefix, suffix)


class FreerunProcessEntry(DaemonProcessEntry):
    """ Class presents single configuration entry for the freerun process/bash_driver """

    db_id = ObjectIdField(name='_id', null=True)
    source = StringField(null=True)
    sink = StringField(null=True)
    trigger_frequency = StringField()       # either 'at DoW-HH:MM' or 'every XXX'
    is_on = BooleanField(default=False)     # defines if the schedulable is active or off
    state_machine_name = StringField()

    entry_name = StringField()              # name of the schedulable
    description = StringField()             # description of the schedulable
    event_log = ListField()
    related_unit_of_work = ObjectIdField()

    @classmethod
    def key_fields(cls):
        return cls.process_name.name, cls.entry_name.name

    @property
    def schedulable_name(self):
        return build_schedulable_name(self.process_name, self.entry_name)


def freerun_context_entry(process_name,
                          entry_name,
                          classname,
                          token,
                          trigger_frequency,
                          is_on=True,
                          present_on_boxes=None,
                          description=None,
                          arguments=None,
                          exchange=EXCHANGE_FREERUN_WORKER,
                          queue=None,
                          routing=None,
                          pid_file=None,
                          log_file=None):
    """ forms process context entry """
    _ROUTING_PREFIX = 'routing_'
    _QUEUE_PREFIX = 'queue_'
    _SUFFIX = '_freerun'

    if arguments is not None:
        assert isinstance(arguments, dict)

    process_entry = FreerunProcessEntry(
        process_name=process_name,
        entry_name=entry_name,
        trigger_frequency=trigger_frequency,
        state_machine_name=STATE_MACHINE_FREERUN,
        is_on=is_on,
        classname=classname,
        token=token,
        present_on_boxes=present_on_boxes,
        description=description,
        mq_queue=queue if queue is not None else _QUEUE_PREFIX + token + _SUFFIX,
        mq_routing_key=routing if routing is not None else _ROUTING_PREFIX + token + _SUFFIX,
        mq_exchange=exchange,
        arguments=arguments if arguments is not None else dict(),
        log_filename=log_file if log_file is not None else token + _SUFFIX + '.log',
        pid_filename=pid_file if pid_file is not None else token + _SUFFIX + '.pid')
    return process_entry


ENTRY_NAME = FreerunProcessEntry.entry_name.name
