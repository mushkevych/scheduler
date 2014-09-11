__author__ = 'Bohdan Mushkevych'

from datetime import datetime, timedelta

from db.model import scheduler_entry
from db.dao.scheduler_entry_dao import SchedulerEntryDao
from db.dao.unit_of_work_dao import UnitOfWorkDao
from system import time_helper
from system.repeat_timer import RepeatTimer
from system.event_clock import format_time_trigger_string, parse_time_trigger_string
from system.process_context import ProcessContext
from mx.commons import valid_only
from mx.tree_node_details import TreeNodeDetails


class ActionHandler(object):
    def __init__(self, mbean, request):
        self.mbean = mbean
        self.logger = self.mbean.logger
        self.request = request
        self.process_name = request.args.get('process_name')
        self.timeperiod = request.args.get('timeperiod')
        self.valid = self.mbean is not None and self.process_name is not None and self.timeperiod is not None
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.sc_dao = SchedulerEntryDao(self.logger)

    @valid_only
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

    @valid_only
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

    @valid_only
    def action_get_uow(self):
        resp = dict()
        timetable = self.mbean.timetable
        tree = timetable.get_tree(self.process_name)

        if tree is not None:
            time_qualifier = ProcessContext.get_time_qualifier(self.process_name)
            self.timeperiod = time_helper.cast_to_time_qualifier(time_qualifier, self.timeperiod)
            node = tree.get_node_by_process(self.process_name, self.timeperiod)

            uow_id = node.timetable_record.related_unit_of_work
            if uow_id is None:
                resp = {'response': 'no related unit_of_work'}
            else:
                resp = self.uow_dao.get_one(uow_id).document
                for key in resp:
                    resp[key] = str(resp[key])

        return resp

    @valid_only
    def action_get_log(self):
        resp = dict()
        timetable = self.mbean.timetable
        tree = timetable.get_tree(self.process_name)

        if tree is not None:
            time_qualifier = ProcessContext.get_time_qualifier(self.process_name)
            self.timeperiod = time_helper.cast_to_time_qualifier(time_qualifier, self.timeperiod)
            node = tree.get_node_by_process(self.process_name, self.timeperiod)
            resp['log'] = node.timetable_record.log

        return resp

    @valid_only
    def action_change_interval(self):
        resp = dict()
        new_interval = self.request.args.get('interval')
        if new_interval is not None:
            parsed_trigger_time, timer_klass = parse_time_trigger_string(new_interval)
            thread_handler = self.mbean.thread_handlers[self.process_name]
            entry_document = thread_handler.args[1]  # of type SchedulerEntry

            if isinstance(thread_handler, timer_klass):
                # trigger time has changed only frequency of run
                thread_handler.change_interval(parsed_trigger_time)
            else:
                # trigger time requires different type of timer - RepeatTimer instead of EventClock and vice versa
                thread_handler.cancel()
                new_thread_handler = timer_klass(parsed_trigger_time, thread_handler.function, thread_handler.args)
                self.mbean.thread_handlers[self.process_name] = new_thread_handler
                is_active = entry_document.process_state == scheduler_entry.STATE_ON
                if is_active:
                    new_thread_handler.start()

            entry_document.trigger_time = format_time_trigger_string(self.mbean.thread_handlers[self.process_name])
            self.sc_dao.update(entry_document)
            resp['status'] = 'changed interval for %r to %r' % (self.process_name, new_interval)

        return resp

    @valid_only
    def action_trigger_now(self):
        resp = dict()
        thread_handler = self.mbean.thread_handlers[self.process_name]
        thread_handler.trigger()

        if thread_handler.is_alive():
            next_run = timedelta(seconds=thread_handler.interval_current) + thread_handler.activation_dt
            next_run = next_run - datetime.utcnow()
        else:
            next_run = 'NA'

        resp['status'] = 'Triggered process %r; Next run in to %r' % (self.process_name, str(next_run).split('.')[0])
        return resp

    @valid_only
    def action_change_process_state(self):
        resp = dict()
        thread_handler = self.mbean.thread_handlers[self.process_name]
        document = thread_handler.args[1]  # of type SchedulerEntry

        state = self.request.args.get('state')
        if state is None:
            # request was performed with undefined "state", what means that checkbox was unselected
            # thus - turning off the process
            thread_handler.cancel()
            document.process_state = scheduler_entry.STATE_OFF
            message = 'Stopped RepeatTimer for %s' % document.process_name
        elif not thread_handler.is_alive():
            document.process_state = scheduler_entry.STATE_ON

            thread_handler = RepeatTimer(thread_handler.interval_current,
                                         thread_handler.call_back,
                                         thread_handler.args,
                                         thread_handler.kwargs)
            thread_handler.start()

            self.mbean.thread_handlers[self.process_name] = thread_handler
            message = 'Started RepeatTimer for %s, triggering every %d seconds' \
                      % (document.process_name, document.trigger_time)
        else:
            message = 'RepeatTimer for %s is already active. Ignoring request.' % document.process_name

        self.sc_dao.update(document)
        self.logger.info(message)
        resp['status'] = message
        return resp
