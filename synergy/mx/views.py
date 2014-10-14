__author__ = 'Bohdan Mushkevych'

import json

from werkzeug.utils import redirect
from werkzeug.wrappers import Response

from synergy.mx.freerun_action_handler import FreerunActionHandler
from synergy.mx.managed_action_handler import ManagedActionHandler
from synergy.mx.scheduler_entries import SchedulerEntries
from synergy.mx.processing_statements import ProcessingStatementDetails
from synergy.mx.utils import render_template, expose, jinja_env
from synergy.mx.tree_node_details import TreeNodeDetails
from synergy.mx.timeperiod_details import TimeperiodDetails
from synergy.mx.timetable_details import TimetableDetails


@expose('/')
@expose('/scheduler_managed_entries/')
def scheduler_managed_entries(request):
    details = SchedulerEntries(jinja_env.globals['mbean'])
    return render_template('scheduler_managed_entries.html', details=details)


@expose('/scheduler_freerun_entries/')
def scheduler_freerun_entries(request):
    details = SchedulerEntries(jinja_env.globals['mbean'])
    return render_template('scheduler_freerun_entries.html', details=details)


@expose('/edit_schedulable_form/')
def edit_schedulable_form(request):
    handler = FreerunActionHandler(jinja_env.globals['mbean'], request)
    return render_template('schedulable_form.html', handler=handler)


@expose('/new_schedulable_form/')
def new_schedulable_form(request):
    return render_template('schedulable_form.html', handler=None)


@expose('/timetable_details/')
def timetable_details(request):
    details = TimetableDetails(jinja_env.globals['mbean'])
    return render_template('timetable_details.html', details=details)


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


@expose('/action_update_freerun_entry')
def action_update_freerun_entry(request):
    handler = FreerunActionHandler(jinja_env.globals['mbean'], request)
    resp = handler.action_update_managed_entry()
    return Response(response=json.dumps(resp),
                    mimetype='application/json')


@expose('/action_reprocess/')
def action_reprocess(request):
    handler = ManagedActionHandler(jinja_env.globals['mbean'], request)
    return Response(response=json.dumps(handler.action_reprocess()),
                    mimetype='application/json')


@expose('/action_skip/')
def action_skip(request):
    handler = ManagedActionHandler(jinja_env.globals['mbean'], request)
    return Response(response=json.dumps(handler.action_skip()),
                    mimetype='application/json')


@expose('/action_get_uow/')
def action_get_uow(request):
    handler = ManagedActionHandler(jinja_env.globals['mbean'], request)
    return Response(response=json.dumps(handler.action_get_uow()),
                    mimetype='application/json')


@expose('/action_get_log/')
def action_get_log(request):
    handler = ManagedActionHandler(jinja_env.globals['mbean'], request)
    return Response(response=json.dumps(handler.action_get_log()),
                    mimetype='application/json')


@expose('/action_change_managed_interval/')
def action_change_managed_interval(request):
    handler = ManagedActionHandler(jinja_env.globals['mbean'], request)
    handler.action_change_interval()
    return redirect('/')


@expose('/action_change_freerun_interval/')
def action_change_freerun_interval(request):
    handler = FreerunActionHandler(jinja_env.globals['mbean'], request)
    handler.action_change_interval()
    return redirect('/')


@expose('/action_trigger_managed_now/')
def action_trigger_managed_now(request):
    handler = ManagedActionHandler(jinja_env.globals['mbean'], request)
    handler.action_trigger_now()
    return redirect('/scheduler_managed_entries/')


@expose('/action_trigger_freerun_now/')
def action_trigger_freerun_now(request):
    handler = FreerunActionHandler(jinja_env.globals['mbean'], request)
    handler.action_trigger_now()
    return redirect('/scheduler_freerun_entries/')


@expose('/action_change_managed_state/')
def action_change_managed_state(request):
    handler = ManagedActionHandler(jinja_env.globals['mbean'], request)
    handler.action_change_state()
    return redirect('/scheduler_managed_entries/')


@expose('/action_change_freerun_state/')
def action_change_freerun_state(request):
    handler = FreerunActionHandler(jinja_env.globals['mbean'], request)
    handler.action_change_state()
    return redirect('/scheduler_freerun_entries/')


@expose('/object_viewer/')
def object_viewer(request):
    return render_template('object_viewer.html')


# referenced from mx.utils.py module
def processing_details(request):
    return render_template('processing_details.html')


# referenced from mx.synergy_mx.py module
def not_found(request):
    return render_template('not_found.html')
