__author__ = 'Bohdan Mushkevych'

from synergy.conf.process_context import ProcessContext
from synergy.db.model import scheduler_managed_entry
from synergy.system.event_clock import parse_time_trigger_string
from synergy.scheduler.scheduler_constants import *


class ThreadHandlerArguments(object):
    """ ThreadHandlerArgument is a data structure around Thread Handler arguments """

    def __init__(self, key, trigger_time, scheduler_entry_obj, handler_type):
        self.key = key
        self.trigger_time = trigger_time
        self.scheduler_entry_obj = scheduler_entry_obj
        self.handler_type = handler_type


class ThreadHandler(object):
    """ ThreadHandler is a thread running within the Synergy Scheduler and triggering Scheduler's fire_XXX logic"""

    def __init__(self, logger, key, trigger_time, function, scheduler_entry_obj, handler_type):
        self.logger = logger
        self.key = key
        self.trigger_time = trigger_time
        self.function = function
        self.scheduler_entry_obj = scheduler_entry_obj
        self.handler_type = handler_type

        self.timer_instance = None
        self.arguments = ThreadHandlerArguments(key, trigger_time, scheduler_entry_obj, handler_type)
        self.logger.info('Created Synergy Scheduler Thread Handler %r~%r' % (self.key, trigger_time))

    def __del__(self):
        if self.timer_instance is not None:
            self.timer_instance.cancel()

    def _construct_handler(self, key, trigger_time, function, parameters):
        """ method returns either:
         - alarm clock of type EventClock, when <schedule> is in format 'at HH:MM, ..., HH:MM'
         - repeat timer of type RepeatTimer, when <schedule> is in format 'every seconds'
         On trigger event this module triggers call_back function with arguments (args, kwargs)
        """
        parsed_trigger_time, timer_klass = parse_time_trigger_string(trigger_time)
        timer_instance = timer_klass(parsed_trigger_time, function, args=parameters)
        self.logger.info('Created %s for %r with schedule %r' % (timer_klass.__name__, key, trigger_time))
        return timer_instance

    def _activate_handler(self, scheduler_entry_obj, process_name, entry_name, function, handler_type):
        """ method parses scheduler_entry_obj and creates a timer_handler out of it
         timer_handler is enlisted to either :self.freerun_handlers or :self.managed_handlers
         timer_handler is started, unless it is marked as STATE_OFF """
        trigger_time = scheduler_entry_obj.trigger_time
        is_active = scheduler_entry_obj.state == scheduler_managed_entry.STATE_ON

        if handler_type == TYPE_MANAGED:
            handler_key = process_name
            arguments = [handler_key, scheduler_entry_obj, handler_type]
            handler = self._construct_handler(handler_key, trigger_time, function, arguments)
            self.managed_handlers[handler_key] = handler
        elif handler_type == TYPE_FREERUN:
            handler_key = (process_name, entry_name)
            arguments = [handler_key, scheduler_entry_obj, handler_type]
            handler = self._construct_handler(handler_key, trigger_time, function, arguments)
            self.freerun_handlers[handler_key] = handler
        else:
            self.logger.error('Process/Handler type %s is not known to the system. Skipping it.' % handler_type)
            return

        if is_active:
            handler.start()
            self.logger.info('Started scheduler thread for %s:%r.' % (handler_type, handler_key))
        else:
            self.logger.info('Handler for %s:%r registered in Scheduler. Idle until activated.'
                             % (handler_type, handler_key))

    # **************** Scheduler Methods ************************
    def _load_managed_entries(self):
        """ loads scheduler managed entries. no start-up procedures are performed """
        scheduler_entries = self.se_managed_dao.get_all()
        for scheduler_entry_obj in scheduler_entries:
            process_name = scheduler_entry_obj.process_name
            if scheduler_entry_obj.process_name not in ProcessContext.CONTEXT:
                self.logger.error('Process %r is not known to the system. Skipping it.' % process_name)
                continue

            process_type = ProcessContext.get_process_type(process_name)
            if process_type in [TYPE_BLOCKING_DEPENDENCIES, TYPE_BLOCKING_CHILDREN, TYPE_MANAGED]:
                function = self.fire_managed_worker
                handler_type = TYPE_MANAGED
            elif process_type == TYPE_GARBAGE_COLLECTOR:
                function = self.fire_garbage_collector
                handler_type = TYPE_MANAGED
            elif process_type == TYPE_FREERUN:
                self.logger.error('TYPE_FREERUN process %s was found in scheduler_managed_entry table. '
                                  'Move the process to the scheduler_freerun_entry table. Skipping the process.'
                                  % process_type)
                continue
            else:
                self.logger.error('Process type %s is not known to the system. Skipping it.' % process_type)
                continue

            try:
                self._activate_handler(scheduler_entry_obj, process_name, 'NA', function, handler_type)
            except Exception:
                self.logger.error('Scheduler Handler %r failed to start. Skipping it.' % (scheduler_entry_obj.key,))

    def _load_freerun_entries(self):
        """ reads scheduler managed entries and starts their timers to trigger events """
        scheduler_entries = self.se_freerun_dao.get_all()
        for scheduler_entry_obj in scheduler_entries:
            try:
                self._activate_handler(scheduler_entry_obj, scheduler_entry_obj.process_name,
                                       scheduler_entry_obj.entry_name, self.fire_freerun_worker, TYPE_FREERUN)
            except Exception:
                self.logger.error('Scheduler Handler %r failed to start. Skipping it.' % (scheduler_entry_obj.key,))


if __name__ == '__main__':
    from synergy.scheduler.scheduler_constants import PROCESS_SCHEDULER

    source = ThreadHandler(PROCESS_SCHEDULER)
    source.start()
