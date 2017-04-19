__author__ = 'Bohdan Mushkevych'

from synergy.scheduler.scheduler_constants import STATE_MACHINE_FREERUN, EXCHANGE_FREERUN_WORKER
from synergy.db.model.daemon_process_entry import DaemonProcessEntry
from odm.fields import StringField, ListField, ObjectIdField, BooleanField

PROCESS_NAME = 'process_name'           # name of the process to handle the schedulables
ENTRY_NAME = 'entry_name'               # name of the schedulable
DESCRIPTION = 'description'             # description of the schedulable
IS_ON = 'is_on'                         # defines if the schedulable is active or off
TRIGGER_FREQUENCY = 'trigger_frequency'  # either 'at DoW-HH:MM' or 'every XXX'
STATE_MACHINE_NAME = 'state_machine_name'
SOURCE = 'source'
SINK = 'sink'

# contains list of last MAX_NUMBER_OF_EVENTS job events, such as emission of the UOW
EVENT_LOG = 'event_log'
MAX_NUMBER_OF_EVENTS = 128
RELATED_UNIT_OF_WORK = 'related_unit_of_work'


def split_schedulable_name(name):
    return name.split('::', 1)


def build_schedulable_name(prefix, suffix):
    return '{0}::{1}'.format(prefix, suffix)


class FreerunProcessEntry(DaemonProcessEntry):
    """ Class presents single configuration entry for the freerun process/bash_driver """

    db_id = ObjectIdField('_id', null=True)
    source = StringField(SOURCE, null=True)
    sink = StringField(SINK, null=True)
    trigger_frequency = StringField(TRIGGER_FREQUENCY)
    is_on = BooleanField(IS_ON, default=False)
    state_machine_name = StringField(STATE_MACHINE_NAME)

    entry_name = StringField(ENTRY_NAME)
    description = StringField(DESCRIPTION)
    event_log = ListField(EVENT_LOG)
    related_unit_of_work = ObjectIdField(RELATED_UNIT_OF_WORK)

    @property
    def key(self):
        return self.process_name, self.entry_name

    @key.setter
    def key(self, value):
        self.process_name = value[0]
        self.entry_name = value[1]

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
