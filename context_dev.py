__author__ = 'Bohdan Mushkevych'

from db.model.process_context_entry import _process_context_entry
from system.time_qualifier import *
from constants import *


queue_context = {
}


process_context = {
    PROCESS_SITE_DAILY: _process_context_entry(
        process_name=PROCESS_SITE_DAILY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=TOKEN_SITE,
        time_qualifier=QUALIFIER_DAILY,
        exchange=EXCHANGE_VERTICAL,
        process_type=TYPE_VERTICAL_AGGREGATOR),

    PROCESS_SITE_HOURLY: _process_context_entry(
        process_name=PROCESS_SITE_HOURLY,
        classname='workers.site_hourly_aggregator.SiteHourlyAggregator.start',
        token=TOKEN_SITE,
        time_qualifier=QUALIFIER_HOURLY,
        exchange=EXCHANGE_VERTICAL,
        process_type=TYPE_VERTICAL_AGGREGATOR,
        source='single_session'),

    PROCESS_SITE_MONTHLY: _process_context_entry(
        process_name=PROCESS_SITE_MONTHLY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=TOKEN_SITE,
        time_qualifier=QUALIFIER_MONTHLY,
        exchange=EXCHANGE_VERTICAL,
        process_type=TYPE_VERTICAL_AGGREGATOR),

    PROCESS_SITE_YEARLY: _process_context_entry(
        process_name=PROCESS_SITE_YEARLY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=TOKEN_SITE,
        time_qualifier=QUALIFIER_YEARLY,
        exchange=EXCHANGE_VERTICAL,
        process_type=TYPE_VERTICAL_AGGREGATOR),

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
        exchange=EXCHANGE_HORIZONTAL,
        process_type=TYPE_HORIZONTAL_AGGREGATOR),

    PROCESS_CLIENT_MONTHLY: _process_context_entry(
        process_name=PROCESS_CLIENT_MONTHLY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=TOKEN_CLIENT,
        time_qualifier=QUALIFIER_MONTHLY,
        exchange=EXCHANGE_HORIZONTAL,
        process_type=TYPE_HORIZONTAL_AGGREGATOR),

    PROCESS_CLIENT_YEARLY: _process_context_entry(
        process_name=PROCESS_CLIENT_YEARLY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=TOKEN_CLIENT,
        time_qualifier=QUALIFIER_YEARLY,
        exchange=EXCHANGE_HORIZONTAL,
        process_type=TYPE_HORIZONTAL_AGGREGATOR),

    PROCESS_ALERT_DAILY: _process_context_entry(
        process_name=PROCESS_ALERT_DAILY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=TOKEN_ALERT,
        time_qualifier=QUALIFIER_DAILY,
        exchange=EXCHANGE_ALERT,
        process_type=TYPE_ALERT)
}


timetable_context = {
}
