__author__ = 'Bohdan Mushkevych'

try:
    from http.client import NO_CONTENT
except ImportError:
    from httplib import NO_CONTENT

import json
from werkzeug.wrappers import Response
from werkzeug.utils import redirect

from synergy.mx.gc_action_handler import GcActionHandler
from synergy.mx.freerun_action_handler import FreerunActionHandler
from synergy.mx.managed_action_handler import ManagedActionHandler
from synergy.mx.scheduler_entries import SchedulerEntries
from synergy.mx.dashboard_handler import DashboardHandler
from synergy.mx.utils import render_template, expose
from synergy.mx.tree_node_details import TreeNodeDetails
from synergy.mx.tree_details import TreeDetails
from synergy.mx.supervisor_action_handler import SupervisorActionHandler


@expose('/managed/entries/')
def scheduler_managed_entries(request, **values):
    details = SchedulerEntries(request, **values)
    return render_template('scheduler_managed_entries.html', details=details)


@expose('/freerun/entries/')
def scheduler_freerun_entries(request, **values):
    details = SchedulerEntries(request, **values)
    return render_template('scheduler_freerun_entries.html', details=details)


@expose('/')
def landing_page(request, **values):
    return redirect('/managed/dashboard/')


@expose('/managed/dashboard/')
def dashboard_managed(request, **values):
    details = DashboardHandler(request, **values)
    return render_template('dashboard_managed.html', details=details)


@expose('/freerun/dashboard/')
def dashboard_freeruns(request, **values):
    details = DashboardHandler(request, **values)
    return render_template('dashboard_freeruns.html', details=details)


@expose('/trees/')
def details_trees(request, **values):
    details = TreeDetails(request, **values)
    return Response(response=json.dumps(details.mx_page_trees), mimetype='application/json')


@expose('/tree/')
def details_tree(request, **values):
    details = TreeDetails(request, **values)
    return Response(response=json.dumps(details.tree_details), mimetype='application/json')


@expose('/tree/nodes/')
def details_tree_nodes(request, **values):
    details = TreeNodeDetails(request, **values)
    return Response(response=json.dumps(details.details), mimetype='application/json')


@expose('/tree/node/reprocess/')
def reprocess_job(request, **values):
    handler = ManagedActionHandler(request, **values)
    handler.reprocess_tree_node()
    return Response(status=NO_CONTENT)


@expose('/tree/node/skip/')
def skip_job(request, **values):
    handler = ManagedActionHandler(request, **values)
    handler.skip_tree_node()
    return Response(status=NO_CONTENT)


@expose('/freerun/entry/', methods=['HEAD', 'DELETE', 'PUT', 'POST'])
def freerun_entry_action(request, **values):
    handler = FreerunActionHandler(request, **values)
    if 'cancel_button' in handler.request_arguments or request.method == 'HEAD':
        pass
    elif 'insert_button' in handler.request_arguments or request.method == 'PUT':
        handler.create_entry()
    elif 'delete_button' in handler.request_arguments or request.method == 'DELETE':
        handler.delete_entry()
    elif 'update_button' in handler.request_arguments or request.method == 'POST':
        handler.update_entry()
    else:
        handler.logger.error(f'MX Error: unsupported method for by /freerun/entry/: {request.method}')
    return Response(status=NO_CONTENT)


@expose('/managed/uow/')
def managed_uow(request, **values):
    handler = ManagedActionHandler(request, **values)
    return Response(response=json.dumps(handler.get_uow()), mimetype='application/json')


@expose('/freerun/uow/')
def freerun_uow(request, **values):
    handler = FreerunActionHandler(request, **values)
    return Response(response=json.dumps(handler.get_uow()), mimetype='application/json')


@expose('/freerun/uow/cancel/')
def freerun_cancel_uow(request, **values):
    handler = FreerunActionHandler(request, **values)
    handler.cancel_uow()
    return Response(status=NO_CONTENT)


# TODO change to /managed/event/log/
@expose('/managed/log/event/')
def managed_event_log(request, **values):
    handler = ManagedActionHandler(request, **values)
    return Response(response=json.dumps(handler.get_event_log()), mimetype='application/json')


# TODO change to /freerun/event/log/
@expose('/freerun/log/event/')
def freerun_event_log(request, **values):
    handler = FreerunActionHandler(request, **values)
    return Response(response=json.dumps(handler.get_event_log()), mimetype='application/json')


# TODO change to /managed/uow/log/
@expose('/managed/log/uow/')
def managed_uow_log(request, **values):
    handler = ManagedActionHandler(request, **values)
    return Response(response=json.dumps(handler.get_uow_log()), mimetype='application/json')


# TODO change to /freerun/uow/log/
@expose('/freerun/log/uow/')
def freerun_uow_log(request, **values):
    handler = FreerunActionHandler(request, **values)
    return Response(response=json.dumps(handler.get_uow_log()), mimetype='application/json')


@expose('/managed/entry/interval/')
def managed_change_interval(request, **values):
    handler = ManagedActionHandler(request, **values)
    handler.change_interval()
    return Response(status=NO_CONTENT)


@expose('/freerun/entry/interval/')
def freerun_change_interval(request, **values):
    handler = FreerunActionHandler(request, **values)
    handler.change_interval()
    return Response(status=NO_CONTENT)


@expose('/managed/entry/trigger/')
def managed_trigger_now(request, **values):
    handler = ManagedActionHandler(request, **values)
    handler.trigger_now()
    return Response(status=NO_CONTENT)


@expose('/freerun/entry/trigger/')
def freerun_trigger_now(request, **values):
    handler = FreerunActionHandler(request, **values)
    handler.trigger_now()
    return Response(status=NO_CONTENT)


@expose('/managed/entry/deactivate/')
def managed_deactivate_trigger(request, **values):
    handler = ManagedActionHandler(request, **values)
    handler.deactivate_trigger()
    return Response(status=NO_CONTENT)


@expose('/freerun/entry/deactivate/')
def freerun_deactivate_trigger(request, **values):
    handler = FreerunActionHandler(request, **values)
    handler.deactivate_trigger()
    return Response(status=NO_CONTENT)


@expose('/managed/entry/activate/')
def managed_activate_trigger(request, **values):
    handler = ManagedActionHandler(request, **values)
    handler.activate_trigger()
    return Response(status=NO_CONTENT)


@expose('/freerun/entry/activate/')
def freerun_activate_trigger(request, **values):
    handler = FreerunActionHandler(request, **values)
    handler.activate_trigger()
    return Response(status=NO_CONTENT)


@expose('/gc/flush/all/')
def gc_flush_all(request, **values):
    handler = GcActionHandler(request, **values)
    handler.flush_all()
    return Response(status=NO_CONTENT)


@expose('/gc/flush/one/')
def gc_flush_one(request, **values):
    handler = GcActionHandler(request, **values)
    handler.flush_one()
    return Response(status=NO_CONTENT)


@expose('/gc/refresh/')
def gc_refresh(request, **values):
    handler = GcActionHandler(request, **values)
    handler.refresh()
    return Response(status=NO_CONTENT)


@expose('/system/gc/log/')
def gc_log(request, **values):
    handler = GcActionHandler(request, **values)
    return Response(response=json.dumps(handler.tail_gc_log()), mimetype='application/json')


@expose('/system/mx/log/')
def mx_log(request, **values):
    handler = SchedulerEntries(request, **values)
    return Response(response=json.dumps(handler.tail_mx_log()), mimetype='application/json')


@expose('/system/scheduler/log/')
def scheduler_log(request, **values):
    handler = SchedulerEntries(request, **values)
    return Response(response=json.dumps(handler.tail_scheduler_log()), mimetype='application/json')


@expose('/supervisor/entries/')
def supervisor_entries(request, **values):
    handler = SupervisorActionHandler(request, **values)
    return render_template('supervisor_entries.html', details=handler.entries)


@expose('/supervisor/entry/start/')
def supervisor_start_process(request, **values):
    handler = SupervisorActionHandler(request, **values)
    handler.mark_for_start()
    return Response(status=NO_CONTENT)


@expose('/supervisor/entry/stop/')
def supervisor_stop_process(request, **values):
    handler = SupervisorActionHandler(request, **values)
    handler.mark_for_stop()
    return Response(status=NO_CONTENT)


@expose('/viewer/object/')
def object_viewer(request, **values):
    return render_template('object_viewer.html')


@expose('/viewer/schedulable/')
def schedulable_viewer(request, **values):
    if 'is_new_entry' in request.args and request.args['is_new_entry'] in ('True', 'true', '1'):
        handler = None
    else:
        handler = FreerunActionHandler(request, **values)
    return render_template('schedulable_form.html', handler=handler)


@expose('/mx_page_tiles/')
def mx_page_tiles(request, **values):
    details = TreeDetails(request, **values)
    return render_template('mx_page_tiles.html', details=details)


# referenced from mx.synergy_mx.py module
def not_found(request, **values):
    return render_template('not_found.html')
