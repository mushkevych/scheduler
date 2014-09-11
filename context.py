__author__ = 'Bohdan Mushkevych'

from system.time_qualifier import *
from constants import *
from scheduler.scheduler_constants import *
from supervisor.supervisor_constants import *
from db.model.queue_context_entry import _queue_context_entry
from db.model.process_context_entry import _process_context_entry
from db.model.timetable_entry import _timetable_entry

queue_context = {
    QUEUE_REQUESTED_PACKAGES: _queue_context_entry(exchange=EXCHANGE_FREERUN_WORKER,
                                                   queue_name=QUEUE_REQUESTED_PACKAGES),
}


process_context = {
    PROCESS_GC: _process_context_entry(
        process_name=PROCESS_GC,
        classname='workers.garbage_collector_worker.GarbageCollectorWorker.start',
        token=TOKEN_GC,
        time_qualifier=QUALIFIER_BY_SCHEDULE,
        exchange=EXCHANGE_UTILS,
        process_type=TYPE_GARBAGE_COLLECTOR,
        source='units_of_work',
        sink='units_of_work'),

    PROCESS_SCHEDULER: _process_context_entry(
        process_name=PROCESS_SCHEDULER,
        classname='scheduler.synergy_scheduler.Scheduler.start',
        token=TOKEN_SCHEDULER,
        time_qualifier='',
        queue='',
        routing='',
        exchange=''),

    PROCESS_SUPERVISOR: _process_context_entry(
        process_name=PROCESS_SUPERVISOR,
        classname='supervisor.synergy_supervisor.Supervisor.start',
        token=TOKEN_SUPERVISOR,
        time_qualifier='',
        queue='',
        routing='',
        exchange=''),

    PROCESS_LAUNCH_PY: _process_context_entry(
        process_name=PROCESS_LAUNCH_PY,
        classname='',
        token=TOKEN_LAUNCH_PY,
        time_qualifier=QUALIFIER_REAL_TIME,
        routing=ROUTING_IRRELEVANT,
        exchange=EXCHANGE_UTILS)
}


timetable_context = {
    TREE_SITE_VERTICAL: _timetable_entry(
        tree_name=TREE_SITE_VERTICAL,
        tree_classname='scheduler.tree.FourLevelTree',
        enclosed_processes=[PROCESS_SITE_YEARLY, PROCESS_SITE_MONTHLY, PROCESS_SITE_DAILY, PROCESS_SITE_HOURLY],
        dependent_on=[],
        mx_name=TOKEN_SITE,
        mx_page=MX_PAGE_TRAFFIC),

    TREE_CLIENT_HORIZONTAL: _timetable_entry(
        tree_name=TREE_CLIENT_HORIZONTAL,
        tree_classname='scheduler.tree.ThreeLevelTree',
        enclosed_processes=[PROCESS_CLIENT_YEARLY, PROCESS_CLIENT_MONTHLY, PROCESS_CLIENT_DAILY],
        dependent_on=[TREE_SITE_VERTICAL],
        mx_name=TOKEN_CLIENT,
        mx_page=MX_PAGE_TRAFFIC),

    TREE_LINEAR_DAILY: _timetable_entry(
        tree_name=TREE_CLIENT_HORIZONTAL,
        tree_classname='scheduler.tree.TwoLevelTree',
        enclosed_processes=[PROCESS_ALERT_DAILY],
        dependent_on=[],
        mx_name=TOKEN_ALERT,
        mx_page=MX_PAGE_TRAFFIC)
}


try:
    from settings import ENVIRONMENT
    overrides = __import__('context_' + ENVIRONMENT)
except:
    overrides = __import__('context_dev')

process_context.update(overrides.process_context)
queue_context.update(overrides.queue_context)
timetable_context.update(overrides.timetable_context)
