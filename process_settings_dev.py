from process_context import *


def register_processes():
    process_entry = _process_context_entry(
        process_name=PROCESS_SITE_DAILY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=_TOKEN_SITE,
        time_qualifier=ProcessContext.QUALIFIER_DAILY,
        exchange=ProcessContext.EXCHANGE_VERTICAL,
        process_type=TYPE_VERTICAL_AGGREGATOR)
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_SITE_HOURLY,
        classname='workers.site_hourly_aggregator.SiteHourlyAggregator.start',
        token=_TOKEN_SITE,
        time_qualifier=ProcessContext.QUALIFIER_HOURLY,
        exchange=ProcessContext.EXCHANGE_VERTICAL,
        process_type=TYPE_VERTICAL_AGGREGATOR,
        source='single_session'),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_SITE_MONTHLY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=_TOKEN_SITE,
        time_qualifier=ProcessContext.QUALIFIER_MONTHLY,
        exchange=ProcessContext.EXCHANGE_VERTICAL,
        process_type=TYPE_VERTICAL_AGGREGATOR),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_SITE_YEARLY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=_TOKEN_SITE,
        time_qualifier=ProcessContext.QUALIFIER_YEARLY,
        exchange=ProcessContext.EXCHANGE_VERTICAL,
        process_type=TYPE_VERTICAL_AGGREGATOR),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_GC,
        classname='workers.garbage_collector_worker.GarbageCollectorWorker.start',
        token=_TOKEN_GC,
        time_qualifier=ProcessContext.QUALIFIER_BY_SCHEDULE,
        exchange=ProcessContext.EXCHANGE_UTILS,
        process_type=TYPE_GARBAGE_COLLECTOR,
        source='units_of_work',
        sink='units_of_work'),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_SESSION_WORKER_00,
        classname='workers.single_session_worker.SingleSessionWorker.start',
        token=_TOKEN_SESSION,
        time_qualifier=ProcessContext.QUALIFIER_REAL_TIME,
        queue=ProcessContext.QUEUE_RAW_DATA,
        routing=ProcessContext.ROUTING_IRRELEVANT,
        exchange=ProcessContext.EXCHANGE_RAW_DATA,
        source='single_session',
        sink='single_session',
        pid_file='session_worker_00.pid',
        log_file='session_worker_00.log'),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_SCHEDULER,
        classname='scheduler.synergy_scheduler.Scheduler.start',
        token=_TOKEN_SCHEDULER,
        time_qualifier='',
        queue='',
        routing='',
        exchange=''),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_SUPERVISOR,
        classname='supervisor.synergy_supervisor.Supervisor.start',
        token=_TOKEN_SUPERVISOR,
        time_qualifier='',
        queue='',
        routing='',
        exchange=''),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_STREAM_GEN,
        classname='event_stream_generator.event_stream_generator.EventStreamGenerator.start',
        token=_TOKEN_STREAM,
        time_qualifier=ProcessContext.QUALIFIER_REAL_TIME,
        queue=ProcessContext.QUEUE_RAW_DATA,
        routing=ProcessContext.ROUTING_IRRELEVANT,
        exchange=ProcessContext.EXCHANGE_RAW_DATA),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_CLIENT_DAILY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=_TOKEN_CLIENT,
        time_qualifier=ProcessContext.QUALIFIER_DAILY,
        exchange=ProcessContext.EXCHANGE_HORIZONTAL,
        process_type=TYPE_HORIZONTAL_AGGREGATOR),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_CLIENT_MONTHLY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=_TOKEN_CLIENT,
        time_qualifier=ProcessContext.QUALIFIER_MONTHLY,
        exchange=ProcessContext.EXCHANGE_HORIZONTAL,
        process_type=TYPE_HORIZONTAL_AGGREGATOR),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_CLIENT_YEARLY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=_TOKEN_CLIENT,
        time_qualifier=ProcessContext.QUALIFIER_YEARLY,
        exchange=ProcessContext.EXCHANGE_HORIZONTAL,
        process_type=TYPE_HORIZONTAL_AGGREGATOR),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_ALERT_DAILY,
        classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
        token=_TOKEN_ALERT,
        time_qualifier=ProcessContext.QUALIFIER_DAILY,
        exchange=ProcessContext.EXCHANGE_ALERT,
        process_type=TYPE_HORIZONTAL_AGGREGATOR),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_UNIT_TEST,
        classname='',
        token='unit_test',
        time_qualifier=ProcessContext.QUALIFIER_REAL_TIME,
        routing=ProcessContext.ROUTING_IRRELEVANT,
        exchange=ProcessContext.EXCHANGE_UTILS),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_LAUNCH_PY,
        classname='',
        token='launch_py',
        time_qualifier=ProcessContext.QUALIFIER_REAL_TIME,
        routing=ProcessContext.ROUTING_IRRELEVANT,
        exchange=ProcessContext.EXCHANGE_UTILS),
    ProcessContext.put_process_entry(process_entry)
