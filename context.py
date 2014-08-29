__author__ = 'Bohdan Mushkevych'

from system.time_qualifier import *
from constants import *


def register_queues():
    from model.queue_context_entry import _queue_context_entry
    from system.mq_queue_context import QueueContext

    queue_entry = _queue_context_entry(exchange=EXCHANGE_HORIZONTAL,
                                       queue_name=QUEUE_REQUESTED_PACKAGES)
    QueueContext.put_context_entry(queue_entry)


def register_processes():
    from model.process_context_entry import _process_context_entry
    from system.process_context import ProcessContext

    process_entry = _process_context_entry(
        process_name=PROCESS_GC,
        classname='workers.garbage_collector_worker.GarbageCollectorWorker.start',
        token=_TOKEN_GC,
        time_qualifier=QUALIFIER_BY_SCHEDULE,
        exchange=EXCHANGE_UTILS,
        process_type=TYPE_GARBAGE_COLLECTOR,
        source='units_of_work',
        sink='units_of_work'),
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
        process_name=PROCESS_UNIT_TEST,
        classname='',
        token='unit_test',
        time_qualifier=QUALIFIER_REAL_TIME,
        routing=ROUTING_IRRELEVANT,
        exchange=EXCHANGE_UTILS),
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_LAUNCH_PY,
        classname='',
        token='launch_py',
        time_qualifier=QUALIFIER_REAL_TIME,
        routing=ROUTING_IRRELEVANT,
        exchange=EXCHANGE_UTILS),
    ProcessContext.put_process_entry(process_entry)


try:
    from settings import ENVIRONMENT
    overrides = __import__('context_' + ENVIRONMENT)
except:
    overrides = __import__('context_dev')
overrides.register_queues()
overrides.register_processes()
