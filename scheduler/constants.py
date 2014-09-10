__author__ = 'Bohdan Mushkevych'

# Scheduler constants
LAG_5_MINUTES = 5  # lag in minutes between finish of the timeperiod and beginning of its processing
PROCESS_SCHEDULER = 'Scheduler'
TOKEN_SCHEDULER = 'scheduler'

# make sure MX_PAGE_XXX refers to exposed URLs from mx.views.py module
MX_PAGE_TRAFFIC = 'traffic_details'
MX_PAGE_FINANCIAL = 'financial_details'

# State Machine (a.k.a. Pipeline) names
PIPELINE_CONTINUOUS = 'continuous'
PIPELINE_DISCRETE = 'continuous'
PIPELINE_SIMPLIFIED_DISCRETE = 'simplified_discrete'

# Supported worker types
TYPE_MANAGED_WORKER = 'type_managed_worker'
TYPE_FREERUN_WORKER = 'type_freerun_worker'
TYPE_GARBAGE_COLLECTOR = 'type_gc'
TYPE_BLOCKING_DEPENDENCIES_WORKER = 'type_blocking_dependencies_worker'
TYPE_BLOCKING_CHILDREN_WORKER = 'type_blocking_children_worker'

