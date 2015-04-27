__author__ = 'Bohdan Mushkevych'

from constants import *
from settings import ENVIRONMENT
from synergy.system.time_qualifier import *
from synergy.scheduler.scheduler_constants import *
from synergy.workers.worker_constants import *
from synergy.db.model.queue_context_entry import queue_context_entry
from synergy.db.model.daemon_process_entry import daemon_context_entry
from synergy.db.model.managed_process_entry import managed_context_entry
from synergy.db.model.timetable_tree_entry import timetable_tree_entry


mq_queue_context = {
    QUEUE_REQUESTED_PACKAGES: queue_context_entry(exchange=EXCHANGE_FREERUN_WORKER,
                                                  queue_name=QUEUE_REQUESTED_PACKAGES),
}


process_context = {
    PROCESS_LAUNCH_PY: daemon_context_entry(
        process_name=PROCESS_LAUNCH_PY,
        classname='',
        token=TOKEN_LAUNCH_PY,
        routing=ROUTING_IRRELEVANT,
        exchange=EXCHANGE_UTILS),

    PROCESS_SITE_DAILY: managed_context_entry(
        process_name=PROCESS_SITE_DAILY,
        classname='workers.site_daily_aggregator.SiteDailyAggregator.start',
        token=TOKEN_SITE,
        time_qualifier=QUALIFIER_DAILY,
        source=COLLECTION_SITE_HOURLY,
        sink=COLLECTION_SITE_DAILY,
        state_machine_name=STATE_MACHINE_DISCRETE,
        blocking_type=BLOCKING_CHILDREN,
        run_on_active_timeperiod=True,
        trigger_frequency='every 900',
        present_on_boxes=['dev.*']),

    PROCESS_SITE_HOURLY: managed_context_entry(
        process_name=PROCESS_SITE_HOURLY,
        classname='workers.site_hourly_aggregator.SiteHourlyAggregator.start',
        token=TOKEN_SITE,
        time_qualifier=QUALIFIER_HOURLY,
        source=COLLECTION_SINGLE_SESSION,
        sink=COLLECTION_SITE_HOURLY,
        state_machine_name=STATE_MACHINE_CONTINUOUS,
        blocking_type=BLOCKING_NORMAL,
        trigger_frequency='every 60',
        present_on_boxes=['dev.*']),

    PROCESS_SITE_MONTHLY: managed_context_entry(
        process_name=PROCESS_SITE_MONTHLY,
        classname='workers.site_monthly_aggregator.SiteMonthlyAggregator.start',
        token=TOKEN_SITE,
        time_qualifier=QUALIFIER_MONTHLY,
        source=COLLECTION_SITE_DAILY,
        sink=COLLECTION_SITE_MONTHLY,
        state_machine_name=STATE_MACHINE_DISCRETE,
        blocking_type=BLOCKING_CHILDREN,
        trigger_frequency='every 10800',
        present_on_boxes=['dev.*']),

    PROCESS_SITE_YEARLY: managed_context_entry(
        process_name=PROCESS_SITE_YEARLY,
        classname='workers.site_yearly_aggregator.SiteYearlyAggregator.start',
        token=TOKEN_SITE,
        time_qualifier=QUALIFIER_YEARLY,
        source=COLLECTION_SITE_MONTHLY,
        sink=COLLECTION_SITE_YEARLY,
        state_machine_name=STATE_MACHINE_DISCRETE,
        blocking_type=BLOCKING_CHILDREN,
        trigger_frequency='every 21600',
        present_on_boxes=['dev.*']),

    PROCESS_SESSION_WORKER_00: daemon_context_entry(
        process_name=PROCESS_SESSION_WORKER_00,
        classname='workers.single_session_worker.SingleSessionWorker.start',
        token=TOKEN_SESSION,
        queue=QUEUE_RAW_DATA,
        routing=ROUTING_IRRELEVANT,
        exchange=EXCHANGE_RAW_DATA,
        pid_file='session_worker_00.pid',
        log_file='session_worker_00.log',
        present_on_boxes=['dev.*']),

    PROCESS_STREAM_GEN: daemon_context_entry(
        process_name=PROCESS_STREAM_GEN,
        classname='workers.event_stream_generator.EventStreamGenerator.start',
        token=TOKEN_STREAM,
        queue=QUEUE_RAW_DATA,
        routing=ROUTING_IRRELEVANT,
        exchange=EXCHANGE_RAW_DATA,
        present_on_boxes=['dev.*']),

    PROCESS_CLIENT_DAILY: managed_context_entry(
        process_name=PROCESS_CLIENT_DAILY,
        classname='workers.client_daily_aggregator.ClientDailyAggregator.start',
        token=TOKEN_CLIENT,
        time_qualifier=QUALIFIER_DAILY,
        state_machine_name=STATE_MACHINE_SIMPLE_DISCRETE,
        blocking_type=BLOCKING_DEPENDENCIES,
        trigger_frequency='every 10800',
        present_on_boxes=['dev.*']),

    PROCESS_CLIENT_MONTHLY: managed_context_entry(
        process_name=PROCESS_CLIENT_MONTHLY,
        classname='workers.client_monthly_aggregator.ClientMonthlyAggregator.start',
        token=TOKEN_CLIENT,
        time_qualifier=QUALIFIER_MONTHLY,
        state_machine_name=STATE_MACHINE_SIMPLE_DISCRETE,
        blocking_type=BLOCKING_CHILDREN,
        trigger_frequency='every 21600',
        present_on_boxes=['dev.*']),

    PROCESS_CLIENT_YEARLY: managed_context_entry(
        process_name=PROCESS_CLIENT_YEARLY,
        classname='workers.client_yearly_aggregator.ClientYearlyAggregator.start',
        token=TOKEN_CLIENT,
        time_qualifier=QUALIFIER_YEARLY,
        state_machine_name=STATE_MACHINE_SIMPLE_DISCRETE,
        blocking_type=BLOCKING_CHILDREN,
        trigger_frequency='every 43200',
        present_on_boxes=['dev.*']),

    PROCESS_ALERT_DAILY: managed_context_entry(
        process_name=PROCESS_ALERT_DAILY,
        classname='workers.alert_daily_worker.AlertDailyWorker.start',
        token=TOKEN_ALERT,
        time_qualifier=QUALIFIER_DAILY,
        state_machine_name=STATE_MACHINE_SIMPLE_DISCRETE,
        blocking_type=BLOCKING_DEPENDENCIES,
        trigger_frequency='every 21600',
        present_on_boxes=['dev.*']),

    PROCESS_BASH_DRIVER: daemon_context_entry(
        process_name=PROCESS_BASH_DRIVER,
        classname='workers.bash_driver.BashDriver.start',
        token=TOKEN_BASH_DRIVER,
        exchange=EXCHANGE_FREERUN_WORKER,
        process_type=TYPE_FREERUN,
        present_on_boxes=['dev.*']),
}


timetable_context = {
    TREE_SITE_VERTICAL: timetable_tree_entry(
        tree_name=TREE_SITE_VERTICAL,
        enclosed_processes=[PROCESS_SITE_YEARLY, PROCESS_SITE_MONTHLY, PROCESS_SITE_DAILY, PROCESS_SITE_HOURLY],
        dependent_on=[],
        mx_name=TOKEN_SITE,
        mx_page=MX_PAGE_TRAFFIC),

    TREE_CLIENT_HORIZONTAL: timetable_tree_entry(
        tree_name=TREE_CLIENT_HORIZONTAL,
        enclosed_processes=[PROCESS_CLIENT_YEARLY, PROCESS_CLIENT_MONTHLY, PROCESS_CLIENT_DAILY],
        dependent_on=[TREE_SITE_VERTICAL],
        mx_name=TOKEN_CLIENT,
        mx_page=MX_PAGE_TRAFFIC),

    TREE_LINEAR_DAILY: timetable_tree_entry(
        tree_name=TREE_LINEAR_DAILY,
        enclosed_processes=[PROCESS_ALERT_DAILY],
        dependent_on=[],
        mx_name=TOKEN_ALERT,
        mx_page=MX_PAGE_ALERT)
}

# Update current dict with the environment-specific settings
try:
    overrides = __import__('context_' + ENVIRONMENT)
    process_context.update(overrides.process_context)
except:
    pass
