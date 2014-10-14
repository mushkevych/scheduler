__author__ = 'Bohdan Mushkevych'

from synergy.db.dao.scheduler_managed_entry_dao import SchedulerManagedEntryDao
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.dao.scheduler_freerun_entry_dao import SchedulerFreerunEntryDao
from synergy.db.model import scheduler_managed_entry
from synergy.db.model.scheduler_managed_entry import SchedulerManagedEntry
from synergy.db.model.scheduler_freerun_entry import SchedulerFreerunEntry
from synergy.scheduler.scheduler_constants import TYPE_MANAGED, TYPE_FREERUN
from synergy.system.event_clock import format_time_trigger_string, parse_time_trigger_string


class AbstractActionHandler(object):
    def __init__(self, mbean, request):
        self.mbean = mbean
        self.logger = self.mbean.logger
        self.request = request
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.se_freerun_dao = SchedulerFreerunEntryDao(self.logger)
        self.se_managed_dao = SchedulerManagedEntryDao(self.logger)

    def _action_change_interval(self, thread_handler, handler_key, handler_type):
        resp = dict()
        new_interval = self.request.args.get('interval')
        if new_interval is not None:
            parsed_trigger_time, timer_klass = parse_time_trigger_string(new_interval)
            scheduler_entry_obj = thread_handler.args[1]  # of type SchedulerEntry

            if isinstance(thread_handler, timer_klass):
                # trigger time has changed only frequency of run
                thread_handler.change_interval(parsed_trigger_time)
            else:
                # trigger time requires different type of timer - RepeatTimer instead of EventClock and vice versa
                thread_handler.cancel()
                new_thread_handler = timer_klass(parsed_trigger_time, thread_handler.function, thread_handler.args)

                if handler_type == TYPE_MANAGED:
                    self.mbean.managed_handlers[handler_key] = thread_handler
                elif handler_type == TYPE_FREERUN:
                    self.mbean.freerun_handlers[handler_key] = thread_handler
                else:
                    self.logger.error('Process/Handler type %s is not known to the system. Skipping it.' % handler_type)
                    return

                is_active = scheduler_entry_obj.state == scheduler_managed_entry.STATE_ON
                if is_active:
                    new_thread_handler.start()

            if isinstance(scheduler_entry_obj, SchedulerFreerunEntry):
                self.se_freerun_dao.update(scheduler_entry_obj)
            elif isinstance(scheduler_entry_obj, SchedulerManagedEntry):
                self.se_managed_dao.update(scheduler_entry_obj)
            else:
                raise ValueError('Unknown scheduler entry type %s' % type(scheduler_entry_obj).__name__)

            scheduler_entry_obj.trigger_time = format_time_trigger_string(thread_handler)
            resp['status'] = 'changed interval for %r to %r' % (self.process_name, new_interval)

        return resp

    def _action_trigger_now(self, thread_handler, handler_key):
        thread_handler.trigger()

        if thread_handler.is_alive():
            next_run = thread_handler.next_run_in()
        else:
            next_run = 'NA'

        return {'status': 'Triggered process %r; Next run in %r' % (handler_key, str(next_run).split('.')[0])}

    def _action_change_state(self, thread_handler):
        scheduler_entry_obj = thread_handler.args[1]

        state = self.request.args.get('state')
        if state is None:
            # request was performed with undefined "state", what means that checkbox was unselected
            # thus - turning off the thread handler
            thread_handler.cancel()
            scheduler_entry_obj.state = scheduler_managed_entry.STATE_OFF
            message = 'Stopped %s for %s' % (type(thread_handler).__name__, scheduler_entry_obj.process_name)
        elif not thread_handler.is_alive():
            scheduler_entry_obj.state = scheduler_managed_entry.STATE_ON
            thread_handler.start()
            message = 'Started %s for %s with schedule %r' \
                      % (type(thread_handler).__name__,
                         scheduler_entry_obj.process_name,
                         scheduler_entry_obj.trigger_time)
        else:
            message = '%s for %s is already active. Ignoring request.' \
                      % (type(thread_handler).__name__, scheduler_entry_obj.process_name)

        if isinstance(scheduler_entry_obj, SchedulerFreerunEntry):
            self.se_freerun_dao.update(scheduler_entry_obj)
        elif isinstance(scheduler_entry_obj, SchedulerFreerunEntry):
            self.se_managed_dao.update(scheduler_entry_obj)
        else:
            raise ValueError('Unknown scheduler entry type %s' % type(scheduler_entry_obj).__name__)

        self.logger.info(message)
        return {'status': message}

    @property
    def scheduler_entry(self):
        raise NotImplementedError('not implemented yet')

    def action_reprocess(self):
        raise NotImplementedError('not implemented yet')

    def action_skip(self):
        raise NotImplementedError('not implemented yet')

    def action_get_uow(self):
        raise NotImplementedError('not implemented yet')

    def action_get_log(self):
        raise NotImplementedError('not implemented yet')

    def action_change_interval(self):
        raise NotImplementedError('not implemented yet')

    def action_trigger_now(self):
        raise NotImplementedError('not implemented yet')

    def action_change_state(self):
        raise NotImplementedError('not implemented yet')
