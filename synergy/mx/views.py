__author__ = 'Bohdan Mushkevych'

from http import HTTPStatus

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


@expose('/scheduler/managed/entries/')
def scheduler_managed_entries(request, **values):
    details = SchedulerEntries(request, **values)
    return render_template('scheduler_managed_entries.html', details=details)


@expose('/scheduler/freerun/entries/')
def scheduler_freerun_entries(request, **values):
    details = SchedulerEntries(request, **values)
    return render_template('scheduler_freerun_entries.html', details=details)


@expose('/')
def landing_page(request, **values):
    return redirect('/scheduler/dashboard/overview/')


@expose('/scheduler/dashboard/overview/')
def dashboard_overview(request, **values):
    details = DashboardHandler(request, **values)
    return render_template('dashboard_overview.html', details=details)


@expose('/scheduler/dashboard/managed/')
def dashboard_managed(request, **values):
    details = DashboardHandler(request, **values)
    return render_template('dashboard_managed.html', details=details)


@expose('/scheduler/dashboard/freerun/')
def dashboard_freeruns(request, **values):
    details = DashboardHandler(request, **values)
    return render_template('dashboard_freeruns.html', details=details)


@expose('/scheduler/jobs/')
def jobs(request, **values):
    details = DashboardHandler(request, **values)
    return Response(response=json.dumps(details.jobs), mimetype='application/json')


@expose('/scheduler/trees/')
def details_trees(request, **values):
    details = TreeDetails(request, **values)
    return Response(response=json.dumps(details.trees), mimetype='application/json')


@expose('/scheduler/tree/')
def details_tree(request, **values):
    details = TreeDetails(request, **values)
    return Response(response=json.dumps(details.tree_details), mimetype='application/json')


@expose('/scheduler/tree/nodes/')
def details_tree_nodes(request, **values):
    details = TreeNodeDetails(request, **values)
    return Response(response=json.dumps(details.details), mimetype='application/json')


@expose('/scheduler/tree/node/reprocess/')
def reprocess_job(request, **values):
    handler = ManagedActionHandler(request, **values)
    handler.reprocess_tree_node()
    return Response(status=HTTPStatus.NO_CONTENT)


@expose('/scheduler/tree/node/skip/')
def skip_job(request, **values):
    handler = ManagedActionHandler(request, **values)
    handler.skip_tree_node()
    return Response(status=HTTPStatus.NO_CONTENT)


@expose('/scheduler/freerun/entry/', methods=['HEAD', 'DELETE', 'PUT', 'POST'])
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
    return Response(status=HTTPStatus.NO_CONTENT)


@expose('/scheduler/managed/uow/')
def managed_uow(request, **values):
    handler = ManagedActionHandler(request, **values)
    return Response(response=json.dumps(handler.get_uow()), mimetype='application/json')


@expose('/scheduler/managed/uow/log/')
def managed_uow_log(request, **values):
    handler = ManagedActionHandler(request, **values)
    return Response(response=json.dumps(handler.get_uow_log()), mimetype='application/json')


@expose('/scheduler/freerun/uow/')
def freerun_uow(request, **values):
    handler = FreerunActionHandler(request, **values)
    return Response(response=json.dumps(handler.get_uow()), mimetype='application/json')


@expose('/scheduler/freerun/uow/cancel/')
def freerun_cancel_uow(request, **values):
    handler = FreerunActionHandler(request, **values)
    handler.cancel_uow()
    return Response(status=HTTPStatus.NO_CONTENT)


@expose('/scheduler/freerun/uow/log/')
def freerun_uow_log(request, **values):
    handler = FreerunActionHandler(request, **values)
    return Response(response=json.dumps(handler.get_uow_log()), mimetype='application/json')


@expose('/scheduler/managed/timeline/')
def managed_event_log(request, **values):
    handler = ManagedActionHandler(request, **values)
    return Response(response=json.dumps(handler.get_event_log()), mimetype='application/json')


@expose('/scheduler/freerun/timeline/')
def freerun_event_log(request, **values):
    handler = FreerunActionHandler(request, **values)
    return Response(response=json.dumps(handler.get_event_log()), mimetype='application/json')


@expose('/scheduler/managed/entry/interval/')
def managed_change_interval(request, **values):
    handler = ManagedActionHandler(request, **values)
    handler.change_interval()
    return Response(status=HTTPStatus.NO_CONTENT)


@expose('/scheduler/freerun/entry/interval/')
def freerun_change_interval(request, **values):
    handler = FreerunActionHandler(request, **values)
    handler.change_interval()
    return Response(status=HTTPStatus.NO_CONTENT)


@expose('/scheduler/managed/entry/trigger/')
def managed_trigger_now(request, **values):
    handler = ManagedActionHandler(request, **values)
    handler.trigger_now()
    return Response(status=HTTPStatus.NO_CONTENT)


@expose('/scheduler/freerun/entry/trigger/')
def freerun_trigger_now(request, **values):
    handler = FreerunActionHandler(request, **values)
    handler.trigger_now()
    return Response(status=HTTPStatus.NO_CONTENT)


@expose('/scheduler/managed/entry/deactivate/')
def managed_deactivate_trigger(request, **values):
    handler = ManagedActionHandler(request, **values)
    handler.deactivate_trigger()
    return Response(status=HTTPStatus.NO_CONTENT)


@expose('/scheduler/freerun/entry/deactivate/')
def freerun_deactivate_trigger(request, **values):
    handler = FreerunActionHandler(request, **values)
    handler.deactivate_trigger()
    return Response(status=HTTPStatus.NO_CONTENT)


@expose('/scheduler/managed/entry/activate/')
def managed_activate_trigger(request, **values):
    handler = ManagedActionHandler(request, **values)
    handler.activate_trigger()
    return Response(status=HTTPStatus.NO_CONTENT)


@expose('/scheduler/freerun/entry/activate/')
def freerun_activate_trigger(request, **values):
    handler = FreerunActionHandler(request, **values)
    handler.activate_trigger()
    return Response(status=HTTPStatus.NO_CONTENT)


@expose('/scheduler/gc/flush/all/')
def gc_flush_all(request, **values):
    handler = GcActionHandler(request, **values)
    handler.flush_all()
    return Response(status=HTTPStatus.NO_CONTENT)


@expose('/scheduler/gc/flush/one/')
def gc_flush_one(request, **values):
    handler = GcActionHandler(request, **values)
    handler.flush_one()
    return Response(status=HTTPStatus.NO_CONTENT)


@expose('/scheduler/gc/refresh/')
def gc_refresh(request, **values):
    handler = GcActionHandler(request, **values)
    handler.refresh()
    return Response(status=HTTPStatus.NO_CONTENT)


@expose('/scheduler/gc/log/')
def gc_log(request, **values):
    handler = GcActionHandler(request, **values)
    return Response(response=json.dumps(handler.tail_gc_log()), mimetype='application/json')


@expose('/scheduler/mx/log/')
def mx_log(request, **values):
    handler = SchedulerEntries(request, **values)
    return Response(response=json.dumps(handler.tail_mx_log()), mimetype='application/json')


@expose('/scheduler/scheduler/log/')
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
    return Response(status=HTTPStatus.NO_CONTENT)


@expose('/supervisor/entry/stop/')
def supervisor_stop_process(request, **values):
    handler = SupervisorActionHandler(request, **values)
    handler.mark_for_stop()
    return Response(status=HTTPStatus.NO_CONTENT)


@expose('/scheduler/viewer/object/')
def object_viewer(request, **values):
    return render_template('object_viewer.html')


@expose('/scheduler/viewer/schedulable/')
def schedulable_viewer(request, **values):
    if 'is_new_entry' in request.args and request.args['is_new_entry'] in ('True', 'true', '1'):
        handler = None
    else:
        handler = FreerunActionHandler(request, **values)
    return render_template('schedulable_form.html', handler=handler)


@expose('/scheduler/mx_page_tiles/')
def mx_page_tiles(request, **values):
    details = TreeDetails(request, **values)
    return render_template('mx_page_tiles.html', details=details)


# referenced from mx.synergy_mx.py module
def not_found(request, **values):
    return render_template('not_found.html')
