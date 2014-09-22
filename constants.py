__author__ = 'Bohdan Mushkevych'


# List of Processes
PROCESS_GC = 'GarbageCollectorWorker'
PROCESS_LAUNCH_PY = 'LaunchPy'      # process provides <process context> to the launch.py script
PROCESS_BASH_DRIVER = 'BashDriver'
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

# List of MX_PAGE_XXX, which are rendered by :mx.views.processing_details function
MX_PAGE_TRAFFIC = 'traffic_details'
MX_PAGE_FINANCIAL = 'financial_details'

# List of Timetable trees
TREE_SITE_VERTICAL = 'tree_site_vertical'
TREE_CLIENT_HORIZONTAL = 'tree_client_horizontal'
TREE_LINEAR_DAILY = 'tree_linear_daily'

# Process tokens. There should be one token per one Timetable tree or stand-alone process
TOKEN_GC = 'gc'
TOKEN_LAUNCH_PY = 'launch_py'
TOKEN_BASH_DRIVER = 'bash'
TOKEN_STREAM = 'stream'
TOKEN_SESSION = 'session'
TOKEN_SITE = 'site'
TOKEN_CLIENT = 'client'
TOKEN_ALERT = 'alert'

# ** QUEUES **
PREFIX_ROUTING = 'routing_'
PREFIX_QUEUE = 'queue_'

QUEUE_REQUESTED_PACKAGES = 'q_requested_package'
QUEUE_RAW_DATA = 'q_raw_data'
ROUTING_IRRELEVANT = 'routing_irrelevant'

EXCHANGE_RAW_DATA = 'ex_raw_data'
EXCHANGE_MANAGED_WORKER = 'ex_managed_worker'
EXCHANGE_FREERUN_WORKER = 'ex_freerun_worker'
EXCHANGE_UTILS = 'ex_utils'
