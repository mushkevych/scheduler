__author__ = 'Bohdan Mushkevych'

from synergy.db.model.freerun_process_entry import FreerunProcessEntry
from synergy.db.model.managed_process_entry import ManagedProcessEntry
from synergy.db.dao.freerun_process_dao import FreerunProcessDao
from synergy.db.dao.managed_process_dao import ManagedProcessDao
from synergy.system.time_trigger_factory import parse_time_trigger_string


class ThreadHandlerHeader(object):
    """ ThreadHandlerHeader is a data structure representing key Thread Handler features.
        It is passed to the Timer instance and later on - to the Scheduler's running function as an argument """

    def __init__(self, key, trigger_frequency, process_entry):
        self.key = key
        self.trigger_frequency = trigger_frequency
        self.process_entry = process_entry


class AbstractThreadHandler(object):
    """ ThreadHandler is a thread running within the Synergy Scheduler and triggering Scheduler's fire_XXX logic"""

    def __init__(self, logger, key, trigger_frequency, call_back, process_entry):
        assert isinstance(process_entry, (FreerunProcessEntry, ManagedProcessEntry))

        self.logger = logger
        self.key = key
        self.trigger_frequency = trigger_frequency
        self.call_back = call_back
        self.process_entry = process_entry

        parsed_trigger_frequency, timer_klass = parse_time_trigger_string(trigger_frequency)
        self.timer_instance = timer_klass(parsed_trigger_frequency, call_back, args=[self.header])
        self.is_started = False
        self.is_terminated = False

        self.logger.info('Created {0} for {1}~{2}'.format(self.__class__.__name__, key, trigger_frequency))

    def __del__(self):
        self.timer_instance.cancel()

    @property
    def header(self):
        return ThreadHandlerHeader(self.key, self.trigger_frequency, self.process_entry)

    @property
    def dao(self):
        raise NotImplementedError(f'property dao must be implemented by {self.__class__.__name__}')

    def activate(self, update_persistent=True):
        if self.timer_instance.is_alive():
            return

        if self.is_terminated:
            parsed_trigger_frequency, timer_klass = parse_time_trigger_string(self.trigger_frequency)
            self.timer_instance = timer_klass(parsed_trigger_frequency, self.call_back, args=[self.header])

        self.process_entry.is_on = True
        if update_persistent:
            self.dao.update(self.process_entry)

        self.timer_instance.start()
        self.is_terminated = False
        self.is_started = True

    def deactivate(self, update_persistent=True):
        self.timer_instance.cancel()
        self.is_terminated = True

        self.process_entry.is_on = False
        if update_persistent:
            self.dao.update(self.process_entry)

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
            self.timer_instance = timer_klass(parsed_trigger_frequency, self.call_back, args=[self.header])
            self.trigger_frequency = value

            # 3. start if necessary
            if self.process_entry.is_on:
                self.timer_instance.start()
                self.is_terminated = False
                self.is_started = True

        self.process_entry.trigger_frequency = value
        if update_persistent:
            self.dao.update(self.process_entry)

    def next_run_in(self, utc_now=None):
        return self.timer_instance.next_run_in(utc_now)

    @property
    def is_alive(self):
        return self.timer_instance.is_alive()


class FreerunThreadHandler(AbstractThreadHandler):
    def __init__(self, logger, key, trigger_frequency, call_back, process_entry):
        super(FreerunThreadHandler, self).__init__(logger, key, trigger_frequency, call_back, process_entry)
        self.freerun_process_dao = FreerunProcessDao(self.logger)

    @property
    def dao(self):
        return self.freerun_process_dao


class ManagedThreadHandler(AbstractThreadHandler):
    def __init__(self, logger, key, trigger_frequency, call_back, process_entry):
        super(ManagedThreadHandler, self).__init__(logger, key, trigger_frequency, call_back, process_entry)
        self.managed_process_dao = ManagedProcessDao(self.logger)

    @property
    def dao(self):
        return self.managed_process_dao
