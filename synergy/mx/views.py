__author__ = 'Bohdan Mushkevych'

import json
import httplib

from werkzeug.wrappers import Response

from synergy.mx.freerun_action_handler import FreerunActionHandler
from synergy.mx.managed_action_handler import ManagedActionHandler
from synergy.mx.scheduler_entries import SchedulerEntries
from synergy.mx.dashboard_handler import DashboardHandler
from synergy.mx.utils import render_template, expose
from synergy.mx.tree_node_details import TreeNodeDetails
from synergy.mx.tree_details import TreeDetails


@expose('/entries/managed/')
def scheduler_managed_entries(request, **values):
    details = SchedulerEntries(request, **values)
    return render_template('scheduler_managed_entries.html', details=details)


@expose('/entries/freerun/')
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


@expose('/')
@expose('/dashboard/managed/')
def dashboard_managed(request, **values):
    details = DashboardHandler(request, **values)
    return render_template('dashboard_managed.html', details=details)


@expose('/dashboard/freeruns/')
def dashboard_freeruns(request, **values):
    details = DashboardHandler(request, **values)
    return render_template('dashboard_freeruns.html', details=details)


@expose('/details/tree_nodes/')
def details_tree_nodes(request, **values):
    details = TreeNodeDetails(request, **values)
    return Response(response=json.dumps(details.details),
                    mimetype='application/json')


@expose('/details/trees/')
def details_trees(request, **values):
    details = TreeDetails(request, **values)
    return Response(response=json.dumps(details.mx_page_entries),
                    mimetype='application/json')


@expose('/action/update_freerun_entry/')
def action_update_freerun_entry(request, **values):
    handler = FreerunActionHandler(request, **values)
    handler.action_update_entry()
    return Response(status=httplib.NO_CONTENT)


@expose('/action/reprocess/')
def action_reprocess(request, **values):
    handler = ManagedActionHandler(request, **values)
    handler.action_reprocess()
    return Response(status=httplib.NO_CONTENT)


@expose('/action/skip/')
def action_skip(request, **values):
    handler = ManagedActionHandler(request, **values)
    handler.action_skip()
    return Response(status=httplib.NO_CONTENT)


@expose('/action/cancel_uow/')
def action_cancel_uow(request, **values):
    handler = FreerunActionHandler(request, **values)
    handler.action_cancel_uow()
    return Response(status=httplib.NO_CONTENT)


@expose('/action/get_uow/')
def action_get_uow(request, **values):
    handler = get_action_handler(request, **values)
    return Response(response=json.dumps(handler.action_get_uow()),
                    mimetype='application/json')


@expose('/action/get_log/')
def action_get_log(request, **values):
    handler = get_action_handler(request, **values)
    return Response(response=json.dumps(handler.action_get_log()),
                    mimetype='application/json')


@expose('/action/change_interval/')
def action_change_interval(request, **values):
    handler = get_action_handler(request, **values)
    handler.action_change_interval()
    return Response(status=httplib.NO_CONTENT)


@expose('/action/trigger_now/')
def action_trigger_now(request, **values):
    handler = get_action_handler(request, **values)
    handler.action_trigger_now()
    return Response(status=httplib.NO_CONTENT)


@expose('/action/deactivate_trigger/')
def action_deactivate_trigger(request, **values):
    handler = get_action_handler(request, **values)
    handler.action_deactivate_trigger()
    return Response(status=httplib.NO_CONTENT)


@expose('/action/activate_trigger/')
def action_activate_trigger(request, **values):
    handler = get_action_handler(request, **values)
    handler.action_activate_trigger()
    return Response(status=httplib.NO_CONTENT)


def get_action_handler(request, **values):
    if 'is_freerun' in request.args and request.args['is_freerun'] in ('True', 'true', '1'):
        handler = FreerunActionHandler(request, **values)
    else:
        handler = ManagedActionHandler(request, **values)
    return handler


@expose('/object_viewer/')
def object_viewer(request, **values):
    return render_template('object_viewer.html')


@expose('/mx_page_tiles/')
def mx_page_tiles(request, **values):
    details = TreeDetails(request, **values)
    return render_template('mx_page_tiles.html', details=details.mx_page_entries)


# referenced from mx.synergy_mx.py module
def not_found(request, **values):
    return render_template('not_found.html')
