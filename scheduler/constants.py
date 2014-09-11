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
PIPELINE_DISCRETE = 'discrete'
PIPELINE_SIMPLIFIED_DISCRETE = 'simplified_discrete'

# Supported worker types
TYPE_MANAGED = 'type_managed'
TYPE_FREERUN = 'type_freerun'
TYPE_GARBAGE_COLLECTOR = 'type_gc'
TYPE_BLOCKING_DEPENDENCIES = 'type_blocking_dependencies'
TYPE_BLOCKING_CHILDREN = 'type_blocking_children'
