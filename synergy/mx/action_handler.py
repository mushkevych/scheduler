__author__ = 'Bohdan Mushkevych'

import json

from synergy.db.dao.scheduler_managed_entry_dao import SchedulerManagedEntryDao
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.dao.scheduler_freerun_entry_dao import SchedulerFreerunEntryDao
from synergy.db.model import scheduler_managed_entry
from synergy.db.model.scheduler_freerun_entry import SchedulerFreerunEntry
from synergy.scheduler.scheduler_constants import TYPE_MANAGED, TYPE_FREERUN
from synergy.system import time_helper
from synergy.system.event_clock import format_time_trigger_string, parse_time_trigger_string
from synergy.conf.process_context import ProcessContext
from synergy.mx.mx_decorators import managed_entry_request, freerun_entry_request
from synergy.mx.tree_node_details import TreeNodeDetails


class ActionHandler(object):
    def __init__(self, mbean, request):
        self.mbean = mbean
        self.logger = self.mbean.logger
        self.request = request
        self.process_name = request.args.get('process_name')
        self.entry_name = request.args.get('entry_name')
        self.timeperiod = request.args.get('timeperiod')
        self.is_managed_request_valid = self.mbean is not None \
                                        and self.process_name is not None \
                                        and self.timeperiod is not None
        self.is_freerun_request_valid = self.mbean is not None \
                                        and self.process_name is not None \
                                        and self.entry_name is not None

        if self.is_managed_request_valid:
            self.process_name = self.process_name.strip()
            self.timeperiod = self.timeperiod.strip()

        if self.is_freerun_request_valid:
            self.process_name = self.process_name.strip()
            self.entry_name = self.entry_name.strip()

        self.uow_dao = UnitOfWorkDao(self.logger)
        self.se_managed_dao = SchedulerManagedEntryDao(self.logger)
        self.se_freerun_dao = SchedulerFreerunEntryDao(self.logger)

    @managed_entry_request
    def action_reprocess(self):
        resp = dict()
        timetable = self.mbean.timetable
        tree = timetable.get_tree(self.process_name)

        if tree is not None:
            time_qualifier = ProcessContext.get_time_qualifier(self.process_name)
            self.timeperiod = time_helper.cast_to_time_qualifier(time_qualifier, self.timeperiod)
            node = tree.get_node_by_process(self.process_name, self.timeperiod)
            self.logger.info('MX (requesting re-process timeperiod %r for %r) {' % (self.timeperiod, self.process_name))
            effected_nodes = node.request_reprocess()
            for node in effected_nodes:
                resp[node.timeperiod] = TreeNodeDetails.get_details(self.logger, node)
            self.logger.info('}')

        return resp

    @managed_entry_request
    def action_skip(self):
        resp = dict()
        timetable = self.mbean.timetable
        tree = timetable.get_tree(self.process_name)

        if tree is not None:
            time_qualifier = ProcessContext.get_time_qualifier(self.process_name)
            self.timeperiod = time_helper.cast_to_time_qualifier(time_qualifier, self.timeperiod)
            node = tree.get_node_by_process(self.process_name, self.timeperiod)
            self.logger.info('MX (requesting skip timeperiod %r for %r) { ' % (self.timeperiod, self.process_name))
            effected_nodes = node.request_skip()
            for node in effected_nodes:
                resp[node.timeperiod] = TreeNodeDetails.get_details(self.logger, node)
            self.logger.info('}')

        return resp

    @managed_entry_request
    def action_get_uow(self):
        resp = dict()
        timetable = self.mbean.timetable
        tree = timetable.get_tree(self.process_name)

        if tree is not None:
            time_qualifier = ProcessContext.get_time_qualifier(self.process_name)
            self.timeperiod = time_helper.cast_to_time_qualifier(time_qualifier, self.timeperiod)
            node = tree.get_node_by_process(self.process_name, self.timeperiod)

            uow_id = node.job_record.related_unit_of_work
            if uow_id is None:
                resp = {'response': 'no related unit_of_work'}
            else:
                resp = self.uow_dao.get_one(uow_id).document
                for key in resp:
                    resp[key] = str(resp[key])

        return resp

    @managed_entry_request
    def action_get_log(self):
        resp = dict()
        timetable = self.mbean.timetable
        tree = timetable.get_tree(self.process_name)

        if tree is not None:
            time_qualifier = ProcessContext.get_time_qualifier(self.process_name)
            self.timeperiod = time_helper.cast_to_time_qualifier(time_qualifier, self.timeperiod)
            node = tree.get_node_by_process(self.process_name, self.timeperiod)
            resp['log'] = node.job_record.log

        return resp

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
            elif isinstance(scheduler_entry_obj, SchedulerFreerunEntry):
                self.se_managed_dao.update(scheduler_entry_obj)
            else:
                raise ValueError('Unknown scheduler entry type %s' % type(scheduler_entry_obj).__name__)

            scheduler_entry_obj.trigger_time = format_time_trigger_string(thread_handler)
            resp['status'] = 'changed interval for %r to %r' % (self.process_name, new_interval)

        return resp

    @managed_entry_request
    def action_change_managed_interval(self):
        thread_handler = self.mbean.managed_handlers[self.process_name]
        return self._action_change_interval(thread_handler, self.process_name, TYPE_MANAGED)

    @freerun_entry_request
    def action_change_freerun_interval(self):
        handler_key = (self.process_name, self.entry_name)
        thread_handler = self.mbean.freerun_handlers[handler_key]
        return self._action_change_interval(thread_handler, handler_key, TYPE_FREERUN)

    def _action_trigger_now(self, thread_handler, handler_key):
        thread_handler.trigger()

        if thread_handler.is_alive():
            next_run = thread_handler.next_run_in()
        else:
            next_run = 'NA'

        return {'status': 'Triggered process %r; Next run in %r' % (handler_key, str(next_run).split('.')[0])}

    @managed_entry_request
    def action_trigger_managed_now(self):
        thread_handler = self.mbean.managed_handlers[self.process_name]
        return self._action_trigger_now(thread_handler, self.process_name)

    @freerun_entry_request
    def action_trigger_freerun_now(self):
        handler_key = (self.process_name, self.entry_name)
        thread_handler = self.mbean.freerun_handlers[handler_key]
        return self._action_trigger_now(thread_handler, handler_key)

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

    @managed_entry_request
    def action_change_managed_state(self):
        thread_handler = self.mbean.managed_handlers[self.process_name]
        return self._action_change_state(thread_handler)

    @freerun_entry_request
    def action_change_freerun_state(self):
        thread_handler = self.mbean.freerun_handlers[(self.process_name, self.entry_name)]
        return self._action_change_state(thread_handler)

    @freerun_entry_request
    def action_update_freerun_entry(self):
        handler_key = (self.process_name, self.entry_name)

        if 'insert_button' in self.request.args:
            scheduler_entry_obj = SchedulerFreerunEntry()
            scheduler_entry_obj.process_name = self.process_name
            scheduler_entry_obj.entry_name = self.entry_name

            if self.request.args['arguments']:
                arguments = self.request.args['arguments'].decode('unicode-escape')
                scheduler_entry_obj.arguments = json.loads(arguments)
            else:
                scheduler_entry_obj.arguments = {}

            scheduler_entry_obj.description = self.request.args['description']
            scheduler_entry_obj.state = self.request.args['state']
            scheduler_entry_obj.trigger_time = self.request.args['trigger_time']
            self.se_freerun_dao.update(scheduler_entry_obj)

            self.mbean._activate_handler(scheduler_entry_obj, self.process_name, self.entry_name,
                                         self.mbean.fire_freerun_worker, TYPE_FREERUN)
        elif 'update_button' in self.request.args:
            thread_handler = self.mbean.freerun_handlers[handler_key]
            scheduler_entry_obj = thread_handler.args[1]

            is_interval_changed = scheduler_entry_obj.trigger_time != self.request.args['trigger_time']
            is_state_changed = scheduler_entry_obj.state != self.request.args['state']

            if self.request.args['arguments']:
                arguments = self.request.args['arguments'].decode('unicode-escape')
                scheduler_entry_obj.arguments = json.loads(arguments)
            else:
                scheduler_entry_obj.arguments = {}

            scheduler_entry_obj.description = self.request.args['description']
            scheduler_entry_obj.state = self.request.args['state']
            scheduler_entry_obj.trigger_time = self.request.args['trigger_time']
            self.se_freerun_dao.update(scheduler_entry_obj)

            if is_interval_changed:
                self._action_change_interval(thread_handler, handler_key, TYPE_FREERUN)
            if is_state_changed:
                self._action_change_state(thread_handler)

        elif 'delete_button' in self.request.args:
            self.se_freerun_dao.remove(handler_key)
            thread_handler = self.mbean.freerun_handlers[handler_key]
            thread_handler.cancel()
            del self.mbean.freerun_handlers[handler_key]

        elif 'cancel_button' in self.request.args:
            pass
        else:
            self.logger.error('Unknown action requested by schedulable_form.html')

        return {'status': 'OK'}

    @freerun_entry_request
    def get_freerun_entry(self):
        handler_key = (self.process_name, self.entry_name)
        thread_handler = self.mbean.freerun_handlers[handler_key]
        scheduler_entry_obj = thread_handler.args[1]
        assert isinstance(scheduler_entry_obj, SchedulerFreerunEntry)
        return scheduler_entry_obj.document
