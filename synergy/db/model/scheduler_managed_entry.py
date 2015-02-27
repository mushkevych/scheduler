__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, ObjectIdField

from synergy.db.model.process_context_entry import ProcessContextEntry
from synergy.scheduler.scheduler_constants import BLOCKING_CHILDREN, BLOCKING_DEPENDENCIES, BLOCKING_NORMAL


PROCESS_NAME = 'process_name'
STATE = 'state'
TRIGGER_TIME = 'trigger_time'
STATE_MACHINE_NAME = 'state_machine_name'
BLOCKING_TYPE = 'blocking_type'

STATE_ON = 'state_on'
STATE_OFF = 'state_off'


class SchedulerManagedEntry(ProcessContextEntry):
    """ Class presents single configuration entry for scheduler managed (i.e. - non-freerun) processes. """
    db_id = ObjectIdField('_id', null=True)
    trigger_time = StringField(TRIGGER_TIME)
    state = StringField(STATE, choices=[STATE_ON, STATE_OFF])
    state_machine_name = StringField(STATE_MACHINE_NAME)
    blocking_type = StringField(BLOCKING_TYPE, choices=[BLOCKING_CHILDREN, BLOCKING_DEPENDENCIES, BLOCKING_NORMAL])

    @BaseDocument.key.getter
    def key(self):
        return self.process_name

    @key.setter
    def key(self, value):
        self.process_name = value


def managed_context_entry(process_name,
                          classname,
                          token,
                          time_qualifier,
                          exchange,
                          trigger_time,
                          state_machine_name,
                          state=STATE_ON,
                          blocking_type=BLOCKING_NORMAL,
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

    process_entry = SchedulerManagedEntry(
        process_name=process_name,
        trigger_time=trigger_time,
        state_machine_name=state_machine_name,
        state=state,
        blocking_type=blocking_type,
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
