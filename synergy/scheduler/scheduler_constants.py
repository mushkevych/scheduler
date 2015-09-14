# Scheduler constants
LAG_5_MINUTES = 5  # lag in minutes between finish of the timeperiod and beginning of its processing

PROCESS_SCHEDULER = 'Scheduler'
PROCESS_GC = 'GarbageCollector'
PROCESS_MX = 'MX'

TOKEN_SCHEDULER = 'scheduler'
TOKEN_GC = 'gc'
TOKEN_WERKZEUG = 'werkzeug'

# State Machine names
STATE_MACHINE_RECOMPUTING = 'recomputing'
STATE_MACHINE_CONTINUOUS = 'continuous'
STATE_MACHINE_DISCRETE = 'discrete'
STATE_MACHINE_FREERUN = 'freerun'

# dependency allows processing of the dependent timeperiod,
# however finalization of the dependent timeperiod is not allowed until all blocking timeperiods are processed
BLOCKING_NORMAL = 'blocking_normal'

# any processing of dependent timeperiods is blocked until blocking timeperiods are processed.
BLOCKING_DEPENDENCIES = 'blocking_dependencies'

# any processing of higher time granularity is blocked until all children timeperiods are processed
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
