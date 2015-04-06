from synergy.db.model.queue_context_entry import queue_context_entry
from synergy.scheduler.scheduler_constants import PROCESS_GC, TOKEN_GC, EXCHANGE_UTILS, TYPE_GARBAGE_COLLECTOR, \
    PROCESS_SCHEDULER, TOKEN_SCHEDULER, QUEUE_UOW_REPORT
from synergy.supervisor.supervisor_constants import PROCESS_SUPERVISOR, TOKEN_SUPERVISOR
from synergy.db.model.daemon_process_entry import daemon_context_entry
from synergy.db.model.managed_process_entry import managed_context_entry
from synergy.system.time_qualifier import QUALIFIER_BY_SCHEDULE


process_context = {
    PROCESS_GC: managed_context_entry(
        process_name=PROCESS_GC,
        classname='synergy.workers.garbage_collector_worker.GarbageCollectorWorker.start',
        token=TOKEN_GC,
        exchange=EXCHANGE_UTILS,
        process_type=TYPE_GARBAGE_COLLECTOR,
        time_qualifier=QUALIFIER_BY_SCHEDULE,
        state_machine_name=None,
        trigger_frequency='every 900'),

    PROCESS_SCHEDULER: daemon_context_entry(
        process_name=PROCESS_SCHEDULER,
        classname='synergy.scheduler.synergy_scheduler.Scheduler.start',
        token=TOKEN_SCHEDULER,
        queue='',
        routing='',
        exchange=''),

    PROCESS_SUPERVISOR: daemon_context_entry(
        process_name=PROCESS_SUPERVISOR,
        classname='synergy.supervisor.synergy_supervisor.Supervisor.start',
        token=TOKEN_SUPERVISOR,
        queue='',
        routing='',
        exchange=''),
}


mq_queue_context = {
    QUEUE_UOW_REPORT: queue_context_entry(exchange=EXCHANGE_UTILS, queue_name=QUEUE_UOW_REPORT),
}


timetable_context = {
}
