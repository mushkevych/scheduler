__author__ = 'Bohdan Mushkevych'

import json
import httplib

from werkzeug.utils import redirect
from werkzeug.wrappers import Response

from synergy.mx.freerun_action_handler import FreerunActionHandler
from synergy.mx.managed_action_handler import ManagedActionHandler
from synergy.mx.scheduler_entries import SchedulerEntries
from synergy.mx.dashboard_handler import DashboardHandler
from synergy.mx.utils import render_template, expose, jinja_env
from synergy.mx.tree_node_details import TreeNodeDetails
from synergy.mx.tree_details import TreeDetails


@expose('/scheduler_managed_entries/')
def scheduler_managed_entries(request, **values):
    details = SchedulerEntries(request, **values)
    return render_template('scheduler_managed_entries.html', details=details)


@expose('/scheduler_freerun_entries/')
def scheduler_freerun_entries(request, **values):
    details = SchedulerEntries(request, **values)
    return render_template('scheduler_freerun_entries.html', details=details)


@expose('/open_schedulable_form/')
def open_schedulable_form(request, **values):
    if 'is_new_entry' in request.args and request.args['is_new_entry'] in ('True', 'true', '1'):
        handler = None
    else:
        handler = FreerunActionHandler(request, **values)
    return render_template('schedulable_form.html', handler=handler)


@expose('/timetable_details/')
def timetable_details(request, **values):
    details = TreeDetails(request, **values)
    return render_template('timetable_details.html', details=details.timetable_entries)


@expose('/')
@expose('/dashboard_managed/')
def dashboard_managed(request, **values):
    details = DashboardHandler(request, **values)
    return render_template('dashboard_managed.html', details=details)


@expose('/dashboard_freeruns/')
def dashboard_freeruns(request, **values):
    details = DashboardHandler(request, **values)
    return render_template('dashboard_freeruns.html', details=details)


@expose('/request_tree_nodes/')
def request_tree_nodes(request, **values):
    details = TreeNodeDetails(request, **values)
    return Response(response=json.dumps(details.details),
                    mimetype='application/json')


@expose('/request_trees/')
def request_trees(request, **values):
    details = TreeDetails(request, **values)
    return Response(response=json.dumps(details.mx_page_entries),
                    mimetype='application/json')


@expose('/action_update_freerun_entry/')
def action_update_freerun_entry(request, **values):
    handler = FreerunActionHandler(request, **values)
    handler.action_update_entry()
    return Response(status=httplib.NO_CONTENT)


@expose('/action_reprocess/')
def action_reprocess(request, **values):
    handler = ManagedActionHandler(request, **values)
    handler.action_reprocess()
    return Response(status=httplib.NO_CONTENT)


@expose('/action_skip/')
def action_skip(request, **values):
    handler = ManagedActionHandler(request, **values)
    handler.action_skip()
    return Response(status=httplib.NO_CONTENT)


@expose('/action_cancel_uow/')
def action_cancel_uow(request, **values):
    handler = FreerunActionHandler(request, **values)
    handler.action_cancel_uow()
    return Response(status=httplib.NO_CONTENT)


@expose('/action_get_uow/')
def action_get_uow(request, **values):
    handler, _, _ = preparse_request(request, **values)
    return Response(response=json.dumps(handler.action_get_uow()),
                    mimetype='application/json')


@expose('/action_get_log/')
def action_get_log(request, **values):
    handler, _, _ = preparse_request(request, **values)
    return Response(response=json.dumps(handler.action_get_log()),
                    mimetype='application/json')


@expose('/action_change_interval/')
def action_change_interval(request, **values):
    handler, redirect_target, _ = preparse_request(request, **values)
    handler.action_change_interval()
    return redirect(redirect_target)


@expose('/action_trigger_now/')
def action_trigger_now(request, **values):
    handler, redirect_target, is_batch = preparse_request(request, **values)
    handler.action_trigger_now()
    if not is_batch:
        return redirect(redirect_target)
    else:
        return Response(status=httplib.NO_CONTENT)


@expose('/action_deactivate_trigger/')
def action_deactivate_trigger(request, **values):
    handler, redirect_target, is_batch = preparse_request(request, **values)
    handler.action_deactivate_trigger()
    if not is_batch:
        return redirect(redirect_target)
    else:
        return Response(status=httplib.NO_CONTENT)


@expose('/action_activate_trigger/')
def action_activate_trigger(request, **values):
    handler, redirect_target, is_batch = preparse_request(request, **values)
    handler.action_activate_trigger()
    if not is_batch:
        return redirect(redirect_target)
    else:
        return Response(status=httplib.NO_CONTENT)


def preparse_request(request, **values):
    is_batch = 'is_batch' in request.args and request.args['is_batch'] in ('True', 'true', '1')

    if 'is_freerun' in request.args and request.args['is_freerun'] in ('True', 'true', '1'):
        handler = FreerunActionHandler(request, **values)
        redirect_target = '/scheduler_freerun_entries/'
    else:
        handler = ManagedActionHandler(request, **values)
        redirect_target = '/scheduler_managed_entries/'
    return handler, redirect_target, is_batch


@expose('/object_viewer/')
def object_viewer(request, **values):
    return render_template('object_viewer.html')


@expose('/mx_page_tiles/')
def mx_page_tiles(request, **values):
    return render_template('mx_page_tiles.html')


# referenced from mx.utils.py module
def processing_details(request, **values):
    details = TreeDetails(request, **values)
    return render_template('processing_details.html', details=details.mx_page_entries)


# referenced from mx.synergy_mx.py module
def not_found(request, **values):
    return render_template('not_found.html')
