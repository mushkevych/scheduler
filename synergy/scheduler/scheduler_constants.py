# Scheduler constants
LAG_5_MINUTES = 5  # lag in minutes between finish of the timeperiod and beginning of its processing

PROCESS_SCHEDULER = 'Scheduler'
PROCESS_GC = 'GarbageCollectorWorker'

TOKEN_SCHEDULER = 'scheduler'
TOKEN_GC = 'gc'

# State Machine (a.k.a. Pipeline) names
PIPELINE_CONTINUOUS = 'continuous'
PIPELINE_DISCRETE = 'discrete'
PIPELINE_SIMPLIFIED_DISCRETE = 'simplified_discrete'
PIPELINE_FREERUN = 'freerun'

# Supported worker types
TYPE_MANAGED = 'type_managed'
TYPE_FREERUN = 'type_freerun'
TYPE_GARBAGE_COLLECTOR = 'type_gc'
TYPE_BLOCKING_DEPENDENCIES = 'type_blocking_dependencies'
TYPE_BLOCKING_CHILDREN = 'type_blocking_children'

# MQ exchanges that are required by Synergy Scheduler
EXCHANGE_MANAGED_WORKER = 'ex_managed_worker'
EXCHANGE_FREERUN_WORKER = 'ex_freerun_worker'
EXCHANGE_UTILS = 'ex_utils'

# DB Collection/Table names
COLLECTION_SCHEDULER_MANAGED_ENTRY = 'scheduler_managed_entry'
COLLECTION_SCHEDULER_FREERUN_ENTRY = 'scheduler_freerun_entry'
COLLECTION_UNIT_OF_WORK = 'unit_of_work'

COLLECTION_JOB_HOURLY = 'job_hourly'
COLLECTION_JOB_DAILY = 'job_daily'
COLLECTION_JOB_MONTHLY = 'job_monthly'
COLLECTION_JOB_YEARLY = 'job_yearly'
