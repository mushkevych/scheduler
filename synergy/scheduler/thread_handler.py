__author__ = 'Bohdan Mushkevych'

from synergy.db.model.freerun_process_entry import FreerunProcessEntry
from synergy.db.model.managed_process_entry import ManagedProcessEntry
from synergy.db.dao.freerun_process_dao import FreerunProcessDao
from synergy.db.dao.managed_process_dao import ManagedProcessDao
from synergy.system.event_clock import parse_time_trigger_string
from synergy.scheduler.scheduler_constants import *


def construct_thread_handler(logger, process_entry, call_back):
    """ method parses process_entry and creates a timer_handler out of it """
    trigger_frequency = process_entry.trigger_frequency
    if isinstance(process_entry, ManagedProcessEntry):
        handler_key = process_entry.process_name
        handler_type = TYPE_MANAGED
    elif isinstance(process_entry, FreerunProcessEntry):
        handler_key = (process_entry.process_name, process_entry.entry_name)
        handler_type = TYPE_FREERUN
    else:
        raise ValueError('Scheduler Entry type %s is not known to the system. Skipping it.'
                         % process_entry.__class__.__name__)

    handler = ThreadHandler(logger, handler_key, trigger_frequency, call_back, process_entry, handler_type)
    return handler


class ThreadHandlerArguments(object):
    """ ThreadHandlerArgument is a data structure around Thread Handler arguments.
    It is passed to the Timer instance and later on - to the Scheduler's running function as an argument """

    def __init__(self, key, trigger_frequency, process_entry, handler_type):
        self.key = key
        self.trigger_frequency = trigger_frequency
        self.process_entry = process_entry
        self.handler_type = handler_type


class ThreadHandler(object):
    """ ThreadHandler is a thread running within the Synergy Scheduler and triggering Scheduler's fire_XXX logic"""

    def __init__(self, logger, key, trigger_frequency, call_back, process_entry, handler_type):
        self.logger = logger
        self.key = key
        self.trigger_frequency = trigger_frequency
        self.call_back = call_back
        self.process_entry = process_entry
        self.handler_type = handler_type

        parsed_trigger_frequency, timer_klass = parse_time_trigger_string(trigger_frequency)
        self.timer_instance = timer_klass(parsed_trigger_frequency, call_back, args=[self.callback_args])
        self.is_started = False
        self.is_terminated = False

        self.se_freerun_dao = FreerunProcessDao(self.logger)
        self.se_managed_dao = ManagedProcessDao(self.logger)
        self.logger.info('Created Synergy Scheduler Thread Handler %r~%r' % (key, trigger_frequency))

    def __del__(self):
        self.timer_instance.cancel()

    @property
    def callback_args(self):
        return ThreadHandlerArguments(self.key, self.trigger_frequency, self.process_entry, self.handler_type)

    def _get_dao(self):
        if self.is_managed:
            return self.se_managed_dao
        elif self.is_freerun:
            return self.se_freerun_dao
        else:
            raise ValueError('Scheduler Entry type %s is not known to the system. Skipping it.'
                             % self.process_entry.__class__.__name__)

    def activate(self, update_persistent=True):
        if self.timer_instance.is_alive():
            return

        if self.is_terminated:
            parsed_trigger_frequency, timer_klass = parse_time_trigger_string(self.trigger_frequency)
            self.timer_instance = timer_klass(parsed_trigger_frequency, self.call_back, args=[self.callback_args])

        self.process_entry.is_on = True
        if update_persistent:
            self._get_dao().update(self.process_entry)

        self.timer_instance.start()
        self.is_terminated = False
        self.is_started = True

    def deactivate(self, update_persistent=True):
        self.timer_instance.cancel()
        self.is_terminated = True

        self.process_entry.is_on = False
        if update_persistent:
            self._get_dao().update(self.process_entry)

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
            if self.process_entry.is_on:
                self.timer_instance.start()
                self.is_terminated = False
                self.is_started = True

        self.process_entry.trigger_frequency = value
        if update_persistent:
            self._get_dao().update(self.process_entry)

    def next_run_in(self, utc_now=None):
        return self.timer_instance.next_run_in(utc_now)

    @property
    def is_alive(self):
        return self.timer_instance.is_alive()

    @property
    def is_managed(self):
        return isinstance(self.process_entry, ManagedProcessEntry)

    @property
    def is_freerun(self):
        return isinstance(self.process_entry, FreerunProcessEntry)
