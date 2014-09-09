__author__ = 'Bohdan Mushkevych'


PROCESS_SCHEDULER = 'Scheduler'
PROCESS_SUPERVISOR = 'Supervisor'
PROCESS_GC = 'GarbageCollectorWorker'
PROCESS_STREAM_GEN = 'EventStreamGenerator'
PROCESS_SESSION_WORKER_00 = 'SingleSessionWorker_00'
PROCESS_SESSION_WORKER_01 = 'SingleSessionWorker_01'
PROCESS_SITE_HOURLY = 'SiteHourlyAggregator'
PROCESS_SITE_DAILY = 'SiteDailyAggregator'
PROCESS_SITE_MONTHLY = 'SiteMonthlyAggregator'
PROCESS_SITE_YEARLY = 'SiteYearlyAggregator'
PROCESS_CLIENT_DAILY = 'ClientDailyAggregator'
PROCESS_CLIENT_MONTHLY = 'ClientMonthlyAggregator'
PROCESS_CLIENT_YEARLY = 'ClientYearlyAggregator'
PROCESS_ALERT_DAILY = 'AlertDailyWorker'

# process provides <process context> to unit testing: such as logger, queue, etc
PROCESS_UNIT_TEST = 'UnitTest'

# process provides <process context> to the launch.py script
PROCESS_LAUNCH_PY = 'LaunchPy'

PIPELINE_CONTINUOUS = 'continuous'
PIPELINE_DISCRETE = 'continuous'
PIPELINE_SIMPLIFIED_DISCRETE = 'simplified_discrete'

TYPE_MANAGED_WORKER = 'type_managed_worker'
TYPE_FREERUN_WORKER = 'type_freerun_worker'
TYPE_GARBAGE_COLLECTOR = 'type_gc'
TYPE_BLOCKING_DEPENDENCIES_WORKER = 'type_blocking_dependencies_worker'
TYPE_BLOCKING_CHILDREN_WORKER = 'type_blocking_children_worker'

TREE_SITE_VERTICAL = 'tree_site_vertical'
TREE_CLIENT_HORIZONTAL = 'tree_client_horizontal'
TREE_LINEAR_DAILY = 'tree_linear_daily'

TOKEN_SCHEDULER = 'scheduler'
TOKEN_SUPERVISOR = 'supervisor'
TOKEN_GC = 'gc'
TOKEN_STREAM = 'stream'
TOKEN_SESSION = 'session'
TOKEN_SITE = 'site'
TOKEN_CLIENT = 'client'
TOKEN_ALERT = 'alert'

# ** QUEUES **
PREFIX_ROUTING = 'routing_'
PREFIX_QUEUE = 'queue_'

QUEUE_REQUESTED_PACKAGES = 'q_requested_package'
QUEUE_RAW_DATA = 'queue_raw_data'
ROUTING_IRRELEVANT = 'routing_irrelevant'

EXCHANGE_RAW_DATA = 'exchange_raw_data'
EXCHANGE_VERTICAL = 'exchange_vertical'
EXCHANGE_HORIZONTAL = 'exchange_horizontal'
EXCHANGE_ALERT = 'exchange_alert'
EXCHANGE_UTILS = 'exchange_utils'

# make sure MX_PAGE_XXX refers to exposed URLs from mx.views.py module
MX_PAGE_TRAFFIC = 'traffic_details'
MX_PAGE_FINANCIAL = 'financial_details'
