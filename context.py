__author__ = 'Bohdan Mushkevych'

from constants import *
from system.time_qualifier import *
from scheduler.scheduler_constants import *
from supervisor.supervisor_constants import *
from db.model.queue_context_entry import _queue_context_entry
from db.model.process_context_entry import _process_context_entry
from db.model.timetable_context_entry import _timetable_context_entry


mx_page_context = {
    MX_PAGE_FINANCIAL: 'financial details',
    MX_PAGE_TRAFFIC: 'traffic details',
}


mq_queue_context = {
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
        exchange=EXCHANGE_UTILS),

    PROCESS_SITE_DAILY: _process_context_entry(
        process_name=PROCESS_SITE_DAILY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=TOKEN_SITE,
        time_qualifier=QUALIFIER_DAILY,
        exchange=EXCHANGE_MANAGED_WORKER,
        process_type=TYPE_MANAGED),

    PROCESS_SITE_HOURLY: _process_context_entry(
        process_name=PROCESS_SITE_HOURLY,
        classname='workers.site_hourly_aggregator.SiteHourlyAggregator.start',
        token=TOKEN_SITE,
        time_qualifier=QUALIFIER_HOURLY,
        exchange=EXCHANGE_MANAGED_WORKER,
        process_type=TYPE_MANAGED,
        source='single_session'),

    PROCESS_SITE_MONTHLY: _process_context_entry(
        process_name=PROCESS_SITE_MONTHLY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=TOKEN_SITE,
        time_qualifier=QUALIFIER_MONTHLY,
        exchange=EXCHANGE_MANAGED_WORKER,
        process_type=TYPE_MANAGED),

    PROCESS_SITE_YEARLY: _process_context_entry(
        process_name=PROCESS_SITE_YEARLY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=TOKEN_SITE,
        time_qualifier=QUALIFIER_YEARLY,
        exchange=EXCHANGE_MANAGED_WORKER,
        process_type=TYPE_MANAGED),

    PROCESS_SESSION_WORKER_00: _process_context_entry(
        process_name=PROCESS_SESSION_WORKER_00,
        classname='workers.single_session_worker.SingleSessionWorker.start',
        token=TOKEN_SESSION,
        time_qualifier=QUALIFIER_REAL_TIME,
        queue=QUEUE_RAW_DATA,
        routing=ROUTING_IRRELEVANT,
        exchange=EXCHANGE_RAW_DATA,
        source='single_session',
        sink='single_session',
        pid_file='session_worker_00.pid',
        log_file='session_worker_00.log'),

    PROCESS_STREAM_GEN: _process_context_entry(
        process_name=PROCESS_STREAM_GEN,
        classname='workers.event_stream_generator.EventStreamGenerator.start',
        token=TOKEN_STREAM,
        time_qualifier=QUALIFIER_REAL_TIME,
        queue=QUEUE_RAW_DATA,
        routing=ROUTING_IRRELEVANT,
        exchange=EXCHANGE_RAW_DATA),

    PROCESS_CLIENT_DAILY: _process_context_entry(
        process_name=PROCESS_CLIENT_DAILY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=TOKEN_CLIENT,
        time_qualifier=QUALIFIER_DAILY,
        exchange=EXCHANGE_FREERUN_WORKER,
        process_type=TYPE_MANAGED),

    PROCESS_CLIENT_MONTHLY: _process_context_entry(
        process_name=PROCESS_CLIENT_MONTHLY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=TOKEN_CLIENT,
        time_qualifier=QUALIFIER_MONTHLY,
        exchange=EXCHANGE_FREERUN_WORKER,
        process_type=TYPE_MANAGED),

    PROCESS_CLIENT_YEARLY: _process_context_entry(
        process_name=PROCESS_CLIENT_YEARLY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=TOKEN_CLIENT,
        time_qualifier=QUALIFIER_YEARLY,
        exchange=EXCHANGE_FREERUN_WORKER,
        process_type=TYPE_MANAGED),

    PROCESS_ALERT_DAILY: _process_context_entry(
        process_name=PROCESS_ALERT_DAILY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=TOKEN_ALERT,
        time_qualifier=QUALIFIER_DAILY,
        exchange=EXCHANGE_MANAGED_WORKER,
        process_type=TYPE_BLOCKING_DEPENDENCIES),

    PROCESS_BASH_DRIVER: _process_context_entry(
        process_name=PROCESS_BASH_DRIVER,
        classname='workers.bash_driver.BashDriver.start',
        token=TOKEN_BASH_DRIVER,
        time_qualifier=QUALIFIER_REAL_TIME,
        exchange=EXCHANGE_FREERUN_WORKER,
        process_type=TYPE_FREERUN),
}


timetable_context = {
    TREE_SITE_VERTICAL: _timetable_context_entry(
        tree_name=TREE_SITE_VERTICAL,
        tree_classname='scheduler.tree.FourLevelTree',
        enclosed_processes=[PROCESS_SITE_YEARLY, PROCESS_SITE_MONTHLY, PROCESS_SITE_DAILY, PROCESS_SITE_HOURLY],
        dependent_on=[],
        mx_name=TOKEN_SITE,
        mx_page=MX_PAGE_TRAFFIC),

    TREE_CLIENT_HORIZONTAL: _timetable_context_entry(
        tree_name=TREE_CLIENT_HORIZONTAL,
        tree_classname='scheduler.tree.ThreeLevelTree',
        enclosed_processes=[PROCESS_CLIENT_YEARLY, PROCESS_CLIENT_MONTHLY, PROCESS_CLIENT_DAILY],
        dependent_on=[TREE_SITE_VERTICAL],
        mx_name=TOKEN_CLIENT,
        mx_page=MX_PAGE_TRAFFIC),

    TREE_LINEAR_DAILY: _timetable_context_entry(
        tree_name=TREE_CLIENT_HORIZONTAL,
        tree_classname='scheduler.tree.TwoLevelTree',
        enclosed_processes=[PROCESS_ALERT_DAILY],
        dependent_on=[],
        mx_name=TOKEN_ALERT,
        mx_page=MX_PAGE_TRAFFIC)
}
