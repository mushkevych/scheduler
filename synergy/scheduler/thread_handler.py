__author__ = 'Bohdan Mushkevych'

from synergy.db.model.scheduler_freerun_entry import SchedulerFreerunEntry
from synergy.db.model.scheduler_managed_entry import SchedulerManagedEntry
from synergy.system.event_clock import parse_time_trigger_string
from synergy.scheduler.scheduler_constants import *


def construct_thread_handler(logger, scheduler_entry_obj, call_back):
    """ method parses scheduler_entry_obj and creates a timer_handler out of it """
    trigger_time = scheduler_entry_obj.trigger_time
    if isinstance(scheduler_entry_obj, SchedulerManagedEntry):
        handler_key = scheduler_entry_obj.process_name
        handler_type = TYPE_MANAGED
    elif isinstance(scheduler_entry_obj, SchedulerFreerunEntry):
        handler_key = (scheduler_entry_obj.process_name, scheduler_entry_obj.entry_name)
        handler_type = TYPE_FREERUN
    else:
        raise ValueError('Scheduler Entry type %s is not known to the system. Skipping it.'
                         % scheduler_entry_obj.__class__.__name__)

    handler = ThreadHandler(logger, handler_key, trigger_time, call_back, scheduler_entry_obj, handler_type)
    return handler


class ThreadHandlerArguments(object):
    """ ThreadHandlerArgument is a data structure around Thread Handler arguments """

    def __init__(self, key, trigger_time, scheduler_entry_obj, handler_type):
        self.key = key
        self.trigger_time = trigger_time
        self.scheduler_entry_obj = scheduler_entry_obj
        self.handler_type = handler_type


class ThreadHandler(object):
    """ ThreadHandler is a thread running within the Synergy Scheduler and triggering Scheduler's fire_XXX logic"""

    def __init__(self, logger, key, trigger_time, call_back, scheduler_entry_obj, handler_type):
        self.logger = logger
        self.arguments = ThreadHandlerArguments(key, trigger_time, scheduler_entry_obj, handler_type)

        parsed_trigger_time, timer_klass = parse_time_trigger_string(trigger_time)
        self.timer_instance = timer_klass(parsed_trigger_time, call_back, args=[self.arguments])
        self.is_running = False
        self.is_terminated = False

        self.logger.info('Created Synergy Scheduler Thread Handler %r~%r' % (key, trigger_time))

    def __del__(self):
        self.cancel()

    def start(self):
        self.timer_instance.start()
        self.is_terminated = False
        self.is_running = self.timer_instance.is_alive()

    def cancel(self):
        self.timer_instance.cancel()
        self.is_terminated = True
        self.is_running = self.timer_instance.is_alive()

    def trigger(self):
        self.timer_instance.trigger()
        self.is_terminated = True

    def change_interval(self, value):
        self.timer_instance.change_interval(value)

    def next_run_in(self, utc_now=None):
        return self.timer_instance.next_run_in(utc_now)
