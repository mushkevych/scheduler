__author__ = 'Bohdan Mushkevych'

from synergy.db.model import scheduler_managed_entry
from synergy.scheduler.scheduler_constants import TYPE_MANAGED, TYPE_FREERUN
from synergy.system.event_clock import format_time_trigger_string, parse_time_trigger_string


class AbstractActionHandler(object):
    def __init__(self, mbean, request):
        self.mbean = mbean
        self.logger = self.mbean.logger
        self.request = request

    def _action_change_interval(self, handler_key, handler_type):
        resp = dict()
        new_interval = self.request.args.get('interval')
        if new_interval is not None:
            parsed_trigger_time, timer_klass = parse_time_trigger_string(new_interval)

            thread_handler = self.scheduler_thread_handler()
            if isinstance(thread_handler, timer_klass):
                # trigger time has changed only frequency of run
                thread_handler.change_interval(parsed_trigger_time)
            else:
                # trigger time requires different type of timer - RepeatTimer instead of EventClock and vice versa
                thread_handler.cancel()
                del self.mbean.managed_handlers[handler_key]

                thread_handler = timer_klass(parsed_trigger_time, thread_handler.function, thread_handler.args)
                if handler_type == TYPE_MANAGED:
                    self.mbean.managed_handlers[handler_key] = thread_handler
                elif handler_type == TYPE_FREERUN:
                    self.mbean.freerun_handlers[handler_key] = thread_handler
                else:
                    self.logger.error('Process/Handler type %s is not known to the system. Skipping it.' % handler_type)
                    return

                is_active = self.scheduler_entry().state == scheduler_managed_entry.STATE_ON
                if is_active:
                    thread_handler.start()

            self.scheduler_entry().trigger_time = format_time_trigger_string(thread_handler)
            self.scheduler_entry_dao().update(self.scheduler_entry())
            resp['status'] = 'changed interval for %r to %r' % (thread_handler.args[0], new_interval)

        return resp

    def scheduler_thread_handler(self):
        raise NotImplementedError('not implemented yet')

    def scheduler_entry(self):
        raise NotImplementedError('not implemented yet')

    def scheduler_entry_dao(self):
        raise NotImplementedError('not implemented yet')

    def action_get_uow(self):
        raise NotImplementedError('not implemented yet')

    def action_get_log(self):
        raise NotImplementedError('not implemented yet')

    def action_change_interval(self):
        raise NotImplementedError('not implemented yet')

    def action_trigger_now(self):
        self.scheduler_thread_handler().trigger()

        if self.scheduler_thread_handler().is_alive():
            next_run = self.scheduler_thread_handler().next_run_in()
            next_run = str(next_run).split('.')[0]
        else:
            next_run = 'NA'

        message = 'Triggered process %r; Next run in %s' % (self.scheduler_thread_handler().args[0], next_run)
        self.logger.info(message)
        return {'status': message}

    def action_activate_trigger(self):
        if not self.scheduler_thread_handler().is_alive():
            self.scheduler_entry().state = scheduler_managed_entry.STATE_ON
            self.scheduler_thread_handler().start()
            message = 'Started %s for %s with schedule %r' \
                      % (type(self.scheduler_thread_handler()).__name__,
                         self.scheduler_entry().process_name,
                         self.scheduler_entry().trigger_time)
        else:
            message = '%s for %s is already active. Ignoring request.' \
                      % (type(self.scheduler_thread_handler()).__name__, self.scheduler_entry().process_name)

        self.scheduler_entry_dao().update(self.scheduler_entry())
        self.logger.info(message)
        return {'status': message}

    def action_deactivate_trigger(self):
        if self.scheduler_thread_handler().is_alive():
            self.scheduler_thread_handler().cancel()
            self.scheduler_entry().state = scheduler_managed_entry.STATE_OFF
            message = 'Stopped %s for %s' \
                      % (type(self.scheduler_thread_handler()).__name__, self.scheduler_entry().process_name)
        else:
            message = '%s for %s is already deactivated. Ignoring request.' \
                      % (type(self.scheduler_thread_handler()).__name__, self.scheduler_entry().process_name)

        self.scheduler_entry_dao().update(self.scheduler_entry())
        self.logger.info(message)
        return {'status': message}
