__author__ = 'Bohdan Mushkevych'


# List of Processes
PROCESS_LAUNCH_PY = 'LaunchPy'      # process provides <process context> to the launch.py script
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
PROCESS_SIMPLE_FLOW_DAILY = 'SimpleFlowDaily'

# List of MX_PAGE_XXX, which are rendered by :mx.views.mx_page_tiles function
MX_PAGE_TRAFFIC = 'traffic_details'
MX_PAGE_ALERT = 'alert_details'

# List of Timetable trees
TREE_SITE = 'tree_site'
TREE_CLIENT = 'tree_client'
TREE_ALERT = 'tree_alert'
TREE_FLOW = 'tree_flow'

# Process tokens. There should be one token per one Timetable tree or stand-alone process
TOKEN_LAUNCH_PY = 'launch_py'
TOKEN_STREAM = 'stream'
TOKEN_SESSION = 'session'
TOKEN_SITE = 'site'
TOKEN_CLIENT = 'client'
TOKEN_ALERT = 'alert'
TOKEN_FLOW = 'simple_flow'

# Flow constants
SIMPLE_FLOW_NAME = 'simple_flow'

# *** MQ Constants ***
PREFIX_ROUTING = 'routing_'
PREFIX_QUEUE = 'queue_'

QUEUE_RAW_DATA = 'q_raw_data'
EXCHANGE_RAW_DATA = 'ex_raw_data'
ROUTING_IRRELEVANT = 'routing_irrelevant'

# *** DB Collection/Table names ***
COLLECTION_SINGLE_SESSION = 'single_session'
COLLECTION_SITE_HOURLY = 'site_hourly'
COLLECTION_SITE_DAILY = 'site_daily'
COLLECTION_SITE_MONTHLY = 'site_monthly'
COLLECTION_SITE_YEARLY = 'site_yearly'
COLLECTION_CLIENT_DAILY = 'client_daily'
COLLECTION_CLIENT_MONTHLY = 'client_monthly'
COLLECTION_CLIENT_YEARLY = 'client_yearly'
COLLECTION_ALERT_DAILY = 'alert_daily'
