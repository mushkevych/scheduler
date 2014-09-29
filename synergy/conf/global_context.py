from synergy.scheduler.scheduler_constants import PROCESS_GC, TOKEN_GC, EXCHANGE_UTILS, TYPE_GARBAGE_COLLECTOR, \
    PROCESS_SCHEDULER, TOKEN_SCHEDULER, COLLECTION_UNIT_OF_WORK
from synergy.supervisor.supervisor_constants import PROCESS_SUPERVISOR, TOKEN_SUPERVISOR, COLLECTION_BOX_CONFIGURATION
from synergy.system.time_qualifier import QUALIFIER_BY_SCHEDULE
from synergy.db.model.process_context_entry import _process_context_entry


process_context = {
    PROCESS_GC: _process_context_entry(
        process_name=PROCESS_GC,
        classname='synergy.workers.garbage_collector_worker.GarbageCollectorWorker.start',
        token=TOKEN_GC,
        time_qualifier=QUALIFIER_BY_SCHEDULE,
        exchange=EXCHANGE_UTILS,
        process_type=TYPE_GARBAGE_COLLECTOR,
        source=COLLECTION_UNIT_OF_WORK,
        sink=COLLECTION_UNIT_OF_WORK),

    PROCESS_SCHEDULER: _process_context_entry(
        process_name=PROCESS_SCHEDULER,
        classname='synergy.scheduler.synergy_scheduler.Scheduler.start',
        token=TOKEN_SCHEDULER,
        time_qualifier='',
        queue='',
        routing='',
        exchange=''),

    PROCESS_SUPERVISOR: _process_context_entry(
        process_name=PROCESS_SUPERVISOR,
        classname='synergy.supervisor.synergy_supervisor.Supervisor.start',
        token=TOKEN_SUPERVISOR,
        time_qualifier='',
        queue='',
        routing='',
        exchange='',
        source=COLLECTION_BOX_CONFIGURATION,
        sink=COLLECTION_BOX_CONFIGURATION),
}


mq_queue_context = {
}


timetable_context = {
}
