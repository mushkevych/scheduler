__author__ = 'Bohdan Mushkevych'

from odm.fields import StringField, ObjectIdField, BooleanField, IntegerField

from synergy.db.model.daemon_process_entry import DaemonProcessEntry
from synergy.scheduler.scheduler_constants import BLOCKING_CHILDREN, BLOCKING_DEPENDENCIES, BLOCKING_NORMAL, \
    EXCHANGE_MANAGED_WORKER, TYPE_MANAGED


PROCESS_NAME = 'process_name'
IS_ON = 'is_on'
RUN_ON_ACTIVE_TIMEPERIOD = 'run_on_active_timeperiod'
TRIGGER_FREQUENCY = 'trigger_frequency'
STATE_MACHINE_NAME = 'state_machine_name'
BLOCKING_TYPE = 'blocking_type'
SOURCE = 'source'
SINK = 'sink'
TIME_QUALIFIER = 'time_qualifier'
TIME_GROUPING = 'time_grouping'


class ManagedProcessEntry(DaemonProcessEntry):
    """ Class presents single configuration entry for scheduler managed (i.e. - non-freerun) processes. """
    db_id = ObjectIdField('_id', null=True)
    source = StringField(SOURCE)
    sink = StringField(SINK)
    time_qualifier = StringField(TIME_QUALIFIER)
    time_grouping = IntegerField(TIME_GROUPING)
    trigger_frequency = StringField(TRIGGER_FREQUENCY)
    is_on = BooleanField(IS_ON, default=False)
    run_on_active_timeperiod = BooleanField(RUN_ON_ACTIVE_TIMEPERIOD)
    state_machine_name = StringField(STATE_MACHINE_NAME)
    blocking_type = StringField(BLOCKING_TYPE, choices=[BLOCKING_CHILDREN, BLOCKING_DEPENDENCIES, BLOCKING_NORMAL])

    @DaemonProcessEntry.key.getter
    def key(self):
        return self.process_name

    @DaemonProcessEntry.key.setter
    def key(self, value):
        self.process_name = value


def managed_context_entry(process_name,
                          classname,
                          token,
                          time_qualifier,
                          trigger_frequency,
                          state_machine_name,
                          is_on=True,
                          exchange=EXCHANGE_MANAGED_WORKER,
                          blocking_type=BLOCKING_NORMAL,
                          present_on_boxes=None,
                          time_grouping=1,
                          arguments=None,
                          queue=None,
                          routing=None,
                          process_type=TYPE_MANAGED,
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
        mq_queue=queue,
        mq_routing_key=routing,
        mq_exchange=exchange,
        present_on_boxes=present_on_boxes,
        arguments=arguments,
        time_qualifier=time_qualifier,
        time_grouping=time_grouping,
        process_type=process_type,
        log_filename=log_file,
        pid_filename=pid_file,
        run_on_active_timeperiod=run_on_active_timeperiod)
    return process_entry
