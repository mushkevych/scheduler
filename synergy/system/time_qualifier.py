__author__ = 'Bohdan Mushkevych'

QUALIFIER_REAL_TIME = '_real_time'
QUALIFIER_BY_SCHEDULE = '_by_schedule'
QUALIFIER_HOURLY = '_hourly'
QUALIFIER_DAILY = '_daily'
QUALIFIER_MONTHLY = '_monthly'
QUALIFIER_YEARLY = '_yearly'


# following dict is used by ProcessHierarchy
# for ordering ProcessEntries based on their time_qualifier
QUALIFIER_DICT = {
    QUALIFIER_YEARLY: 1000,
    QUALIFIER_MONTHLY: 900,
    QUALIFIER_DAILY: 800,
    QUALIFIER_HOURLY: 700
}
