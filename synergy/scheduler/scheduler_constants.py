# Scheduler constants
LAG_5_MINUTES = 5  # lag in minutes between finish of the timeperiod and beginning of its processing

PROCESS_SCHEDULER = 'Scheduler'
PROCESS_GC = 'GarbageCollectorWorker'

TOKEN_SCHEDULER = 'scheduler'
TOKEN_GC = 'gc'

# State Machine names
STATE_MACHINE_CONTINUOUS = 'continuous'
STATE_MACHINE_DISCRETE = 'discrete'
STATE_MACHINE_SIMPLE_DISCRETE = 'simple_discrete'
STATE_MACHINE_FREERUN = 'freerun'

# Supported worker types
TYPE_MANAGED = 'type_managed'
TYPE_FREERUN = 'type_freerun'
TYPE_DAEMON = 'type_daemon'
TYPE_GARBAGE_COLLECTOR = 'type_gc'
BLOCKING_NORMAL = 'blocking_normal'
BLOCKING_DEPENDENCIES = 'blocking_dependencies'
BLOCKING_CHILDREN = 'blocking_children'

# MQ exchanges that are required by Synergy Scheduler
EXCHANGE_MANAGED_WORKER = 'ex_managed_worker'
EXCHANGE_FREERUN_WORKER = 'ex_freerun_worker'
EXCHANGE_UTILS = 'ex_utils'

# DB Collection/Table names
COLLECTION_MANAGED_PROCESS = 'managed_process'
COLLECTION_FREERUN_PROCESS = 'freerun_process'
COLLECTION_UNIT_OF_WORK = 'unit_of_work'

COLLECTION_JOB_HOURLY = 'job_hourly'
COLLECTION_JOB_DAILY = 'job_daily'
COLLECTION_JOB_MONTHLY = 'job_monthly'
COLLECTION_JOB_YEARLY = 'job_yearly'

QUEUE_UOW_REPORT = 'q_uow_report'