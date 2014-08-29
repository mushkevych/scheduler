__author__ = 'Bohdan Mushkevych'

from system.time_qualifier import *
from constants import *


def register_processes():
    from model.process_context_entry import _process_context_entry
    from system.process_context import ProcessContext
    process_entry = _process_context_entry(
        process_name=PROCESS_SITE_DAILY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=_TOKEN_SITE,
        time_qualifier=QUALIFIER_DAILY,
        exchange=EXCHANGE_VERTICAL,
        process_type=TYPE_VERTICAL_AGGREGATOR)
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_SITE_HOURLY,
        classname='workers.site_hourly_aggregator.SiteHourlyAggregator.start',
        token=_TOKEN_SITE,
        time_qualifier=QUALIFIER_HOURLY,
        exchange=EXCHANGE_VERTICAL,
        process_type=TYPE_VERTICAL_AGGREGATOR,
        source='single_session'),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_SITE_MONTHLY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=_TOKEN_SITE,
        time_qualifier=QUALIFIER_MONTHLY,
        exchange=EXCHANGE_VERTICAL,
        process_type=TYPE_VERTICAL_AGGREGATOR),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_SITE_YEARLY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=_TOKEN_SITE,
        time_qualifier=QUALIFIER_YEARLY,
        exchange=EXCHANGE_VERTICAL,
        process_type=TYPE_VERTICAL_AGGREGATOR),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_SESSION_WORKER_00,
        classname='workers.single_session_worker.SingleSessionWorker.start',
        token=_TOKEN_SESSION,
        time_qualifier=QUALIFIER_REAL_TIME,
        queue=ProcessContext.QUEUE_RAW_DATA,
        routing=ROUTING_IRRELEVANT,
        exchange=EXCHANGE_RAW_DATA,
        source='single_session',
        sink='single_session',
        pid_file='session_worker_00.pid',
        log_file='session_worker_00.log'),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_STREAM_GEN,
        classname='event_stream_generator.event_stream_generator.EventStreamGenerator.start',
        token=_TOKEN_STREAM,
        time_qualifier=QUALIFIER_REAL_TIME,
        queue=ProcessContext.QUEUE_RAW_DATA,
        routing=ROUTING_IRRELEVANT,
        exchange=EXCHANGE_RAW_DATA),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_CLIENT_DAILY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=_TOKEN_CLIENT,
        time_qualifier=QUALIFIER_DAILY,
        exchange=EXCHANGE_HORIZONTAL,
        process_type=TYPE_HORIZONTAL_AGGREGATOR),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_CLIENT_MONTHLY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=_TOKEN_CLIENT,
        time_qualifier=QUALIFIER_MONTHLY,
        exchange=EXCHANGE_HORIZONTAL,
        process_type=TYPE_HORIZONTAL_AGGREGATOR),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_CLIENT_YEARLY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=_TOKEN_CLIENT,
        time_qualifier=QUALIFIER_YEARLY,
        exchange=EXCHANGE_HORIZONTAL,
        process_type=TYPE_HORIZONTAL_AGGREGATOR),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_ALERT_DAILY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=_TOKEN_ALERT,
        time_qualifier=QUALIFIER_DAILY,
        exchange=EXCHANGE_ALERT,
        process_type=TYPE_ALERT),
    ProcessContext.put_process_entry(process_entry)
