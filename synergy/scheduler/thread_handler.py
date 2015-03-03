__author__ = 'Bohdan Mushkevych'

from synergy.db.model import managed_process_entry
from synergy.db.model.freerun_process_entry import FreerunProcessEntry
from synergy.db.model.managed_process_entry import ManagedProcessEntry
from synergy.db.dao.scheduler_freerun_entry_dao import SchedulerFreerunEntryDao
from synergy.db.dao.scheduler_managed_entry_dao import SchedulerManagedEntryDao
from synergy.system.event_clock import parse_time_trigger_string
from synergy.scheduler.scheduler_constants import *


def construct_thread_handler(logger, scheduler_entry_obj, call_back):
    """ method parses scheduler_entry_obj and creates a timer_handler out of it """
    trigger_frequency = scheduler_entry_obj.trigger_frequency
    if isinstance(scheduler_entry_obj, ManagedProcessEntry):
        handler_key = scheduler_entry_obj.process_name
        handler_type = TYPE_MANAGED
    elif isinstance(scheduler_entry_obj, FreerunProcessEntry):
        handler_key = (scheduler_entry_obj.process_name, scheduler_entry_obj.entry_name)
        handler_type = TYPE_FREERUN
    else:
        raise ValueError('Scheduler Entry type %s is not known to the system. Skipping it.'
                         % scheduler_entry_obj.__class__.__name__)

    handler = ThreadHandler(logger, handler_key, trigger_frequency, call_back, scheduler_entry_obj, handler_type)
    return handler


class ThreadHandlerArguments(object):
    """ ThreadHandlerArgument is a data structure around Thread Handler arguments.
    It is passed to the Timer instance and later on - to the Scheduler's running function as an argument """

    def __init__(self, key, trigger_frequency, scheduler_entry_obj, handler_type):
        self.key = key
        self.trigger_frequency = trigger_frequency
        self.scheduler_entry_obj = scheduler_entry_obj
        self.handler_type = handler_type


class ThreadHandler(object):
    """ ThreadHandler is a thread running within the Synergy Scheduler and triggering Scheduler's fire_XXX logic"""

    def __init__(self, logger, key, trigger_frequency, call_back, scheduler_entry_obj, handler_type):
        self.logger = logger
        self.key = key
        self.trigger_frequency = trigger_frequency
        self.call_back = call_back
        self.scheduler_entry_obj = scheduler_entry_obj
        self.handler_type = handler_type

        parsed_trigger_frequency, timer_klass = parse_time_trigger_string(trigger_frequency)
        self.timer_instance = timer_klass(parsed_trigger_frequency, call_back, args=[self.callback_args])
        self.is_started = False
        self.is_terminated = False

        self.se_freerun_dao = SchedulerFreerunEntryDao(self.logger)
        self.se_managed_dao = SchedulerManagedEntryDao(self.logger)
        self.logger.info('Created Synergy Scheduler Thread Handler %r~%r' % (key, trigger_frequency))

    def __del__(self):
        self.timer_instance.cancel()

    @property
    def callback_args(self):
        return ThreadHandlerArguments(self.key, self.trigger_frequency, self.scheduler_entry_obj, self.handler_type)

    def _get_dao(self):
        if isinstance(self.scheduler_entry_obj, ManagedProcessEntry):
            return self.se_managed_dao
        elif isinstance(self.scheduler_entry_obj, FreerunProcessEntry):
            return self.se_freerun_dao
        else:
            raise ValueError('Scheduler Entry type %s is not known to the system. Skipping it.'
                             % self.scheduler_entry_obj.__class__.__name__)

    def activate(self, update_persistent=True):
        if self.timer_instance.is_alive():
            return

        if self.is_terminated:
            parsed_trigger_frequency, timer_klass = parse_time_trigger_string(self.trigger_frequency)
            self.timer_instance = timer_klass(parsed_trigger_frequency, self.call_back, args=[self.callback_args])

        self.scheduler_entry_obj.state = managed_process_entry.STATE_ON
        if update_persistent:
            self._get_dao().update(self.scheduler_entry_obj)

        self.timer_instance.start()
        self.is_terminated = False
        self.is_started = True

    def deactivate(self, update_persistent=True):
        self.timer_instance.cancel()
        self.is_terminated = True

        self.scheduler_entry_obj.state = managed_process_entry.STATE_OFF
        if update_persistent:
            self._get_dao().update(self.scheduler_entry_obj)

    def trigger(self):
        self.timer_instance.trigger()

    def change_interval(self, value, update_persistent=True):
        parsed_trigger_frequency, timer_klass = parse_time_trigger_string(value)

        if isinstance(self.timer_instance, timer_klass):
            # trigger time has changed only frequency of run
            self.timer_instance.change_interval(parsed_trigger_frequency)
        else:
            # trigger time requires different type of timer - RepeatTimer instead of EventClock or vice versa
            # 1. deactivate current timer
            self.deactivate()

            # 2. create a new timer instance
            parsed_trigger_frequency, timer_klass = parse_time_trigger_string(self.trigger_frequency)
            self.timer_instance = timer_klass(parsed_trigger_frequency, self.call_back, args=[self.callback_args])
            self.trigger_frequency = value

            # 3. start if necessary
            if self.scheduler_entry_obj.state == managed_process_entry.STATE_ON:
                self.timer_instance.start()
                self.is_terminated = False
                self.is_started = True

        self.scheduler_entry_obj.trigger_frequency = value
        if update_persistent:
            self._get_dao().update(self.scheduler_entry_obj)

    def next_run_in(self, utc_now=None):
        return self.timer_instance.next_run_in(utc_now)

    def is_alive(self):
        return self.timer_instance.is_alive()