__author__ = 'Bohdan Mushkevych'

from odm.fields import StringField, ObjectIdField, BooleanField, IntegerField

from synergy.db.model.daemon_process_entry import DaemonProcessEntry
from synergy.scheduler.scheduler_constants import BLOCKING_CHILDREN, BLOCKING_DEPENDENCIES, BLOCKING_NORMAL, \
    EXCHANGE_MANAGED_WORKER, STATE_MACHINE_DISCRETE


PROCESS_NAME = 'process_name'
IS_ON = 'is_on'
TRIGGER_FREQUENCY = 'trigger_frequency'
STATE_MACHINE_NAME = 'state_machine_name'
BLOCKING_TYPE = 'blocking_type'
SOURCE = 'source'
SINK = 'sink'
TIME_QUALIFIER = 'time_qualifier'
TIME_GROUPING = 'time_grouping'


class ManagedProcessEntry(DaemonProcessEntry):
    """ Class presents single configuration entry for scheduler managed (i.e. - non-freerun) processes """
    db_id = ObjectIdField('_id', null=True)
    source = StringField(SOURCE, null=True)
    sink = StringField(SINK, null=True)
    time_qualifier = StringField(TIME_QUALIFIER)
    time_grouping = IntegerField(TIME_GROUPING)
    trigger_frequency = StringField(TRIGGER_FREQUENCY)
    is_on = BooleanField(IS_ON, default=False)
    state_machine_name = StringField(STATE_MACHINE_NAME)
    blocking_type = StringField(BLOCKING_TYPE, choices=[BLOCKING_CHILDREN, BLOCKING_DEPENDENCIES, BLOCKING_NORMAL])

    @property
    def key(self):
        return self.process_name

    @key.setter
    def key(self, value):
        self.process_name = value


def managed_context_entry(process_name,
                          classname,
                          token,
                          time_qualifier,
                          trigger_frequency='every 60',
                          state_machine_name=STATE_MACHINE_DISCRETE,
                          is_on=True,
                          exchange=EXCHANGE_MANAGED_WORKER,
                          blocking_type=BLOCKING_NORMAL,
                          present_on_boxes=None,
                          time_grouping=1,
                          arguments=None,
                          queue=None,
                          routing=None,
                          source=None,
                          sink=None,
                          pid_file=None,
                          log_file=None):
    """ forms process context entry """
    _ROUTING_PREFIX = 'routing_'
    _QUEUE_PREFIX = 'queue_'

    if arguments is not None:
        assert isinstance(arguments, dict)

    process_entry = ManagedProcessEntry(
        process_name=process_name,
        trigger_frequency=trigger_frequency,
        state_machine_name=state_machine_name,
        is_on=is_on,
        blocking_type=blocking_type,
        classname=classname,
        token=token,
        source=source,
        sink=sink,
        mq_queue=queue if queue is not None else _QUEUE_PREFIX + token + time_qualifier,
        mq_routing_key=routing if routing is not None else _ROUTING_PREFIX + token + time_qualifier,
        mq_exchange=exchange,
        present_on_boxes=present_on_boxes,
        arguments=arguments if arguments is not None else dict(),
        time_qualifier=time_qualifier,
        time_grouping=time_grouping,
        log_filename=log_file if log_file is not None else token + time_qualifier + '.log',
        pid_filename=pid_file if pid_file is not None else token + time_qualifier + '.pid')
    return process_entry
