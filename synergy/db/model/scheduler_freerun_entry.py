from synergy.db.model.process_context_entry import ProcessContextEntry

__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, ListField, ObjectIdField

PROCESS_NAME = 'process_name'           # name of the process to handle the schedulables
ENTRY_NAME = 'entry_name'               # name of the schedulable
DESCRIPTION = 'description'             # description of the schedulable
STATE = 'state'                         # either :STATE_ON or :STATE_OFF
TRIGGER_TIME = 'trigger_time'           # either 'at DoW-HH:MM' or 'every XXX'

HISTORIC_LOG = 'historic_log'           # contains list of MAX_NUMBER_OF_LOG_ENTRIES last log messages
MAX_NUMBER_OF_LOG_ENTRIES = 64
RELATED_UNIT_OF_WORK = 'related_unit_of_work'

STATE_ON = 'state_on'
STATE_OFF = 'state_off'


class SchedulerFreerunEntry(ProcessContextEntry):
    """ Class presents single configuration entry for the freerun process/bash_driver . """

    db_id = ObjectIdField('_id', null=True)
    entry_name = StringField(ENTRY_NAME)
    description = StringField(DESCRIPTION)
    trigger_time = StringField(TRIGGER_TIME)
    state = StringField(STATE, choices=[STATE_ON, STATE_OFF])
    log = ListField(HISTORIC_LOG)
    related_unit_of_work = ObjectIdField(RELATED_UNIT_OF_WORK)

    @BaseDocument.key.getter
    def key(self):
        return self.process_name, self.entry_name

    @key.setter
    def key(self, value):
        self.process_name = value[0]
        self.entry_name = value[1]


def freerun_context_entry(process_name,
                          entry_name,
                          classname,
                          token,
                          time_qualifier,
                          exchange,
                          trigger_time,
                          state=STATE_ON,
                          description=None,
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

    process_entry = SchedulerFreerunEntry(
        process_name=process_name,
        entry_name=entry_name,
        trigger_time=trigger_time,
        state=state,
        classname=classname,
        token=token,
        source=source,
        sink=sink,
        description=description,
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
