from scheduler.scheduler_constants import PROCESS_GC, TOKEN_GC, EXCHANGE_UTILS, TYPE_GARBAGE_COLLECTOR, \
    PROCESS_SCHEDULER, TOKEN_SCHEDULER
from supervisor.supervisor_constants import PROCESS_SUPERVISOR, TOKEN_SUPERVISOR
from system.time_qualifier import QUALIFIER_BY_SCHEDULE
from db.model.process_context_entry import _process_context_entry


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
}


mq_queue_context = {
}


timetable_context = {
}
