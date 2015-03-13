__author__ = 'Bohdan Mushkevych'

from synergy.scheduler.scheduler_constants import TYPE_FREERUN, STATE_MACHINE_FREERUN
from synergy.db.model.daemon_process_entry import DaemonProcessEntry
from odm.fields import StringField, ListField, ObjectIdField

PROCESS_NAME = 'process_name'           # name of the process to handle the schedulables
ENTRY_NAME = 'entry_name'               # name of the schedulable
DESCRIPTION = 'description'             # description of the schedulable
STATE = 'state'                         # either :STATE_ON or :STATE_OFF
TRIGGER_FREQUENCY = 'trigger_frequency'  # either 'at DoW-HH:MM' or 'every XXX'
STATE_MACHINE_NAME = 'state_machine_name'
SOURCE = 'source'
SINK = 'sink'

HISTORIC_LOG = 'historic_log'           # contains list of MAX_NUMBER_OF_LOG_ENTRIES last log messages
MAX_NUMBER_OF_LOG_ENTRIES = 64
RELATED_UNIT_OF_WORK = 'related_unit_of_work'

STATE_ON = 'state_on'
STATE_OFF = 'state_off'


class FreerunProcessEntry(DaemonProcessEntry):
    """ Class presents single configuration entry for the freerun process/bash_driver . """

    db_id = ObjectIdField('_id', null=True)
    source = StringField(SOURCE)
    sink = StringField(SINK)
    trigger_frequency = StringField(TRIGGER_FREQUENCY)
    state = StringField(STATE, choices=[STATE_ON, STATE_OFF])
    state_machine_name = StringField(STATE_MACHINE_NAME)

    entry_name = StringField(ENTRY_NAME)
    description = StringField(DESCRIPTION)
    log = ListField(HISTORIC_LOG)
    related_unit_of_work = ObjectIdField(RELATED_UNIT_OF_WORK)

    @DaemonProcessEntry.key.getter
    def key(self):
        return self.process_name, self.entry_name

    @DaemonProcessEntry.key.setter
    def key(self, value):
        self.process_name = value[0]
        self.entry_name = value[1]

    @property
    def is_on(self):
        return self.state == STATE_ON


def freerun_context_entry(process_name,
                          entry_name,
                          classname,
                          token,
                          exchange,
                          trigger_frequency,
                          state=STATE_ON,
                          description=None,
                          arguments=None,
                          queue=None,
                          routing=None,
                          process_type=TYPE_FREERUN,
                          pid_file=None,
                          log_file=None,
                          run_on_active_timeperiod=False):
    """ forms process context entry """
    _ROUTING_PREFIX = 'routing_'
    _QUEUE_PREFIX = 'queue_'
    _SUFFIX = '_freerun'

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

    process_entry = FreerunProcessEntry(
        process_name=process_name,
        entry_name=entry_name,
        trigger_frequency=trigger_frequency,
        time_qualifier=None,
        state_machine_name=STATE_MACHINE_FREERUN,
        state=state,
        classname=classname,
        token=token,
        description=description,
        mq_queue=queue,
        mq_routing_key=routing,
        mq_exchange=exchange,
        arguments=arguments,
        process_type=process_type,
        log_filename=log_file,
        pid_filename=pid_file,
        run_on_active_timeperiod=run_on_active_timeperiod)
    return process_entry
