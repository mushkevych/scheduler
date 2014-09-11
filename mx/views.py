__author__ = 'Bohdan Mushkevych'

import json

from werkzeug.utils import redirect
from werkzeug.wrappers import Response

from mx.action_handler import ActionHandler
from mx.scheduler_entries import SchedulerEntries
from mx.processing_statements import ProcessingStatementDetails
from mx.utils import render_template, expose, jinja_env
from mx.tree_node_details import TreeNodeDetails
from mx.connection_details import ConnectionDetails
from mx.timeperiod_details import TimeperiodDetails
from mx.timetable_details import TimetableDetails


@expose('/')
@expose('/scheduler_details/')
def scheduler_details(request):
    details = SchedulerEntries(jinja_env.globals['mbean'])
    connections = ConnectionDetails(jinja_env.globals['mbean'])
    return render_template('scheduler_entries.html', details=details, connections=connections)


@expose('/timetable_details/')
def timetable_details(request):
    details = TimetableDetails(jinja_env.globals['mbean'])
    return render_template('timetable_details.html', details=details)


@expose('/traffic_details/')
def traffic_details(request):
    return render_template('processing_details.html')


@expose('/financial_details/')
def financial_details(request):
    return render_template('processing_details.html')


@expose('/processing_statements/')
def processing_statements(request):
    details = ProcessingStatementDetails(jinja_env.globals['mbean'], request)
    return render_template('processing_statements.html', details=details)


@expose('/request_children/')
def request_children(request):
    details = TreeNodeDetails(jinja_env.globals['mbean'], request)
    return Response(response=json.dumps(details.details),
                    mimetype='application/json')


@expose('/request_verticals/')
def request_verticals(request):
    details = TimeperiodDetails(jinja_env.globals['mbean'], request)
    return Response(response=json.dumps(details.details),
                    mimetype='application/json')


@expose('/request_timeperiods/')
def request_timeperiods(request):
    details = TimeperiodDetails(jinja_env.globals['mbean'], request)
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


# referenced from mx.synergy_mx.py module
def not_found(request):
    return render_template('not_found.html')
