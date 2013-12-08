__author__ = 'Bohdan Mushkevych'


from db.model import scheduler_configuration
from db.dao.scheduler_configuration_dao import SchedulerConfigurationDao
from db.dao.unit_of_work_dao import UnitOfWorkDao
from datetime import datetime, timedelta
import functools
import json

from werkzeug.utils import cached_property, redirect
from werkzeug.wrappers import Response

from system.repeat_timer import RepeatTimer
from processing_statements import ProcessingStatements
from system.process_context import ProcessContext
from system.performance_ticker import FootprintCalculator
from system import time_helper
from utils import render_template, expose, jinja_env


@expose('/')
@expose('/scheduler_details/')
def scheduler_details(request):
    details = SchedulerDetails(jinja_env.globals['mbean'])
    connections = ConnectionDetails(jinja_env.globals['mbean'])
    return render_template('scheduler_details.html', details=details, connections=connections)


@expose('/traffic_details/')
def traffic_details(request):
    return render_template('processing_details.html')


@expose('/financial_details/')
def financial_details(request):
    return render_template('processing_details.html')


@expose('/processing_statements/')
def processing_statements(request):
    details = TimeperiodProcessingStatements(jinja_env.globals['mbean'], request)
    return render_template('processing_statements.html', details=details)


@expose('/request_children/')
def request_children(request):
    details = NodeDetails(jinja_env.globals['mbean'], request)
    return Response(response=json.dumps(details.details),
                    mimetype='application/json')


@expose('/request_verticals/')
def request_verticals(request):
    details = TimeperiodTreeDetails(jinja_env.globals['mbean'], request)
    return Response(response=json.dumps(details.details),
                    mimetype='application/json')


@expose('/request_timeperiods/')
def request_timeperiods(request):
    details = TimeperiodTreeDetails(jinja_env.globals['mbean'], request)
    return Response(response=json.dumps(details.details),
                    mimetype='application/json')


@expose('/action_reprocess/')
def action_reprocess(request):
    handler = ActionHandler(jinja_env.globals['mbean'], request)
    return Response(response=json.dumps(handler.action_reprocess()),
                    mimetype='application/json')


@expose('/action_skip/')
def action_skip(request):
    handler = ActionHandler(jinja_env.globals['mbean'], request)
    return Response(response=json.dumps(handler.action_skip()),
                    mimetype='application/json')


@expose('/action_get_uow/')
def action_get_uow(request):
    handler = ActionHandler(jinja_env.globals['mbean'], request)
    return Response(response=json.dumps(handler.action_get_uow()),
                    mimetype='application/json')


@expose('/action_get_log/')
def action_get_log(request):
    handler = ActionHandler(jinja_env.globals['mbean'], request)
    return Response(response=json.dumps(handler.action_get_log()),
                    mimetype='application/json')


@expose('/action_change_interval/')
def action_change_interval(request):
    handler = ActionHandler(jinja_env.globals['mbean'], request)
    handler.action_change_interval()
    return redirect('/')


@expose('/action_trigger_now/')
def action_trigger_now(request):
    handler = ActionHandler(jinja_env.globals['mbean'], request)
    handler.action_trigger_now()
    return redirect('/')


@expose('/action_change_process_state/')
def action_change_process_state(request):
    handler = ActionHandler(jinja_env.globals['mbean'], request)
    handler.action_change_process_state()
    return redirect('/')


@expose('/object_viewer/')
def object_viewer(request):
    return render_template('object_viewer.html')


def not_found(request):
    return render_template('not_found.html')


def valid_only(method):
    """ wraps method with verification for _valid_ and """

    @functools.wraps(method)
    def _wrapper(self, *args, **kwargs):
        if not self.valid:
            return dict()
        try:
            return method(self, *args, **kwargs)
        except Exception as e:
            self.logger.error('MX Exception: %s' % str(e), exc_info=True)

    return _wrapper


# Timetable Details views
class TimeperiodTreeDetails(object):
    def __init__(self, mbean, request):
        self.mbean = mbean
        self.logger = self.mbean.logger
        self.request = request
        self.referrer = self.request.referrer
        self.valid = self.mbean is not None

    def _get_reprocessing_details(self, process_name):
        resp = []
        per_process = self.mbean.timetable.reprocess.get(process_name)
        if per_process is not None:
            resp = sorted(per_process.keys())
        return resp

    def _get_nodes_details(self, tree):
        timetable = self.mbean.timetable
        description = dict()
        description['reprocessing_queues'] = dict()
        description['processes'] = dict()
        description['next_timeperiods'] = dict()
        try:
            # workaround for importing "scheduler" package
            if type(tree).__name__ == 'FourLevelTree':
                description['number_of_levels'] = 4
                description['reprocessing_queues']['yearly'] = self._get_reprocessing_details(tree.process_yearly)
                description['reprocessing_queues']['monthly'] = self._get_reprocessing_details(tree.process_monthly)
                description['reprocessing_queues']['daily'] = self._get_reprocessing_details(tree.process_daily)
                description['reprocessing_queues']['hourly'] = self._get_reprocessing_details(tree.process_hourly)
                description['processes']['yearly'] = tree.process_yearly
                description['processes']['monthly'] = tree.process_monthly
                description['processes']['daily'] = tree.process_daily
                description['processes']['hourly'] = tree.process_hourly
                description['next_timeperiods']['yearly'] = timetable.get_next_timetable_record(tree.process_yearly).timeperiod
                description['next_timeperiods']['monthly'] = timetable.get_next_timetable_record(tree.process_monthly).timeperiod
                description['next_timeperiods']['daily'] = timetable.get_next_timetable_record(tree.process_daily).timeperiod
                description['next_timeperiods']['hourly'] = timetable.get_next_timetable_record(tree.process_hourly).timeperiod
                description['type'] = ProcessContext.get_type(tree.process_yearly)
            elif type(tree).__name__ == 'ThreeLevelTree':
                description['number_of_levels'] = 3
                description['reprocessing_queues']['yearly'] = self._get_reprocessing_details(tree.process_yearly)
                description['reprocessing_queues']['monthly'] = self._get_reprocessing_details(tree.process_monthly)
                description['reprocessing_queues']['daily'] = self._get_reprocessing_details(tree.process_daily)
                description['processes']['yearly'] = tree.process_yearly
                description['processes']['monthly'] = tree.process_monthly
                description['processes']['daily'] = tree.process_daily
                description['next_timeperiods']['yearly'] = timetable.get_next_timetable_record(tree.process_yearly).timeperiod
                description['next_timeperiods']['monthly'] = timetable.get_next_timetable_record(tree.process_monthly).timeperiod
                description['next_timeperiods']['daily'] = timetable.get_next_timetable_record(tree.process_daily).timeperiod
                description['type'] = ProcessContext.get_type(tree.process_yearly)
            elif type(tree).__name__ == 'TwoLevelTree':
                description['number_of_levels'] = 1
                description['reprocessing_queues']['linear'] = self._get_reprocessing_details(tree.process_name)
                description['processes']['linear'] = tree.process_name
                description['next_timeperiods']['daily'] = timetable.get_next_timetable_record(tree.process_name).timeperiod
                description['type'] = ProcessContext.get_type(tree.process_name)
        except Exception as e:
            self.logger.error('MX Exception: ' + str(e), exc_info=True)
        finally:
            return description

    @cached_property
    @valid_only
    def details(self):
        """ method iterates thru all trees and visualize only those, that has "mx_page" field set
        to current self.referrer value """
        resp = dict()
        timetable = self.mbean.timetable

        for tree in timetable.trees:
            if tree.mx_page in self.referrer:
                resp[tree.category] = self._get_nodes_details(tree)

        return resp


class NodeDetails(object):
    def __init__(self, mbean, request):
        self.mbean = mbean
        self.logger = self.mbean.logger
        self.request = request
        self.process_name = request.args.get('process_name')
        self.timeperiod = request.args.get('timeperiod')
        self.valid = self.mbean is not None

    @classmethod
    def _get_nodes_details(cls, logger, node):
        """method returns {
                process_name : string,
                timeperiod : string,
                number_of_children : integer,
                number_of_failed_calls : integer,
                state : STATE_SKIPPED, STATE_IN_PROGRESS, STATE_PROCESSED, STATE_FINAL_RUN, STATE_EMBRYO
            }
         """
        description = dict()
        try:
            description['process_name'] = node.process_name
            description['time_qualifier'] = node.time_qualifier
            description['number_of_children'] = len(node.children)
            description['number_of_failed_calls'] = node.timetable_record.number_of_failures
            description['timeperiod'] = node.timetable_record.timeperiod
            description['state'] = node.timetable_record.state
        except Exception as e:
            logger.error('MX Exception: %s' % str(e), exc_info=True)
        finally:
            return description

    @cached_property
    @valid_only
    def details(self):
        resp = dict()
        timetable = self.mbean.timetable
        tree = timetable.get_tree(self.process_name)

        if self.timeperiod is None and tree is not None:
            # return list of yearly nodes
            resp['children'] = dict()
            for key in tree.root.children:
                child = tree.root.children[key]
                resp['children'][key] = NodeDetails._get_nodes_details(self.logger, child)
        elif tree is not None:
            time_qualifier = ProcessContext.get_time_qualifier(self.process_name)
            self.timeperiod = time_helper.cast_to_time_qualifier(time_qualifier, self.timeperiod)
            node = tree.get_node_by_process(self.process_name, self.timeperiod)
            resp['node'] = NodeDetails._get_nodes_details(self.logger, node)
            resp['children'] = dict()
            for key in node.children:
                child = node.children[key]
                resp['children'][key] = NodeDetails._get_nodes_details(self.logger, child)

        return resp


class TimeperiodProcessingStatements(object):
    def __init__(self, mbean, request):
        self.mbean = mbean
        self.logger = self.mbean.logger
        self.request = request
        self.year = self.request.args.get('year')
        self.month = self.request.args.get('month')
        self.day = self.request.args.get('day')
        self.hour = self.request.args.get('hour')
        self.state = self.request.args.get('state')
        if self.state is not None and self.state == 'on':
            self.state = True
        else:
            self.state = False

        if self.year is not None and self.year.strip() == '':
            self.year = None
        if self.month is not None and self.month.strip() == '':
            self.month = None
        if self.day is not None and self.day.strip() == '':
            self.day = None
        self.valid = self.mbean is not None \
            and self.year is not None \
            and self.month is not None \
            and self.day is not None \
            and self.hour is not None

    @cached_property
    @valid_only
    def entries(self):
        processor = ProcessingStatements(self.logger)
        timeperiod = self.year + self.month + self.day + self.hour
        selection = processor.retrieve_for_timeperiod(timeperiod, self.state)
        sorter_keys = sorted(selection.keys())

        resp = []
        for key in sorter_keys:
            t = (key[0], key[1], selection[key].state)
            resp.append(t)

        print ('%r' % resp)
        return resp


class ActionHandler(object):
    def __init__(self, mbean, request):
        self.mbean = mbean
        self.logger = self.mbean.logger
        self.request = request
        self.process_name = request.args.get('process_name')
        self.timeperiod = request.args.get('timeperiod')
        self.valid = self.mbean is not None and self.process_name is not None and self.timeperiod is not None
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.sc_dao = SchedulerConfigurationDao(self.logger)

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
                resp[node.timeperiod] = NodeDetails._get_nodes_details(self.logger, node)
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
                resp[node.timeperiod] = NodeDetails._get_nodes_details(self.logger, node)
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
            resp['log'] = node.timetable_record.log()

        return resp

    @valid_only
    def action_change_interval(self):
        resp = dict()
        new_interval = self.request.args.get('interval')
        if new_interval is not None:
            new_interval = int(new_interval)
            thread_handler = self.mbean.thread_handlers[self.process_name]
            thread_handler.change_interval(new_interval)

            document = thread_handler.args[1]  # of type SchedulerConfigurationEntry
            document.interval = new_interval
            self.sc_dao.update(document)

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
        document = thread_handler.args[1]  # of type SchedulerConfigurationEntry

        state = self.request.args.get('state')
        if state is None:
            # request was performed with undefined "state", what means that checkbox was unselected
            # thus - turning off the process
            thread_handler.cancel()
            document.process_state = scheduler_configuration.STATE_OFF
            message = 'Stopped RepeatTimer for %s' % document.process_name
        elif not thread_handler.is_alive():
            document.process_state = scheduler_configuration.STATE_ON

            thread_handler = RepeatTimer(thread_handler.interval_current,
                                         thread_handler.callable,
                                         thread_handler.args,
                                         thread_handler.kwargs)
            thread_handler.start()

            self.mbean.thread_handlers[self.process_name] = thread_handler
            message = 'Started RepeatTimer for %s, triggering every %d seconds' \
                      % (document.process_name, document.interval)
        else:
            message = 'RepeatTimer for %s is already active. Ignoring request.' % document.process_name

        self.sc_dao.update(document)
        self.logger.info(message)
        resp['status'] = message
        return resp


# Scheduler Details views
class SchedulerDetails(object):
    def __init__(self, mbean):
        self.mbean = mbean
        self.logger = self.mbean.logger

    @cached_property
    def entries(self):
        list_of_rows = []
        try:
            sorter_keys = sorted(self.mbean.thread_handlers.keys())
            for key in sorter_keys:
                row = []
                thread_handler = self.mbean.thread_handlers[key]
                process_name = thread_handler.args[0]
                row.append(process_name)
                row.append(thread_handler.is_alive())
                row.append(int(thread_handler.interval_new))

                if not thread_handler.is_alive():
                    row.append('NA')  # Last Triggered
                    row.append('NA')  # Next Run In
                else:
                    row.append(thread_handler.activation_dt.strftime('%Y-%m-%d %H:%M:%S %Z'))
                    next_run = timedelta(seconds=thread_handler.interval_current) + thread_handler.activation_dt
                    next_run = next_run - datetime.utcnow()
                    row.append(str(next_run).split('.')[0])

                timetable = self.mbean.timetable
                if timetable.get_tree(process_name) is not None:
                    timetable_record = timetable.get_next_timetable_record(process_name)
                    row.append(timetable_record.timeperiod)
                else:
                    row.append('NA')

                # indicate whether process is in active or passive state
                # parameters are set in Scheduler.run() method
                row.append(thread_handler.args[1].process_state == scheduler_configuration.STATE_ON)
                list_of_rows.append(row)
        except Exception as e:
            self.logger.error('MX Exception %s' % str(e), exc_info=True)

        return list_of_rows

    @cached_property
    def footprint(self):
        try:
            calculator = FootprintCalculator()
            footprint = calculator.get_snapshot_as_list()
            return footprint
        except Exception as e:
            self.logger.error('MX Exception %s' % str(e), exc_info=True)


class ConnectionDetails(object):
    def __init__(self, mbean):
        self.mbean = mbean
        self.logger = self.mbean.logger

    @cached_property
    def entries(self):
        list_of_rows = []
        return list_of_rows
