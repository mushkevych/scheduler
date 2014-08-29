__author__ = 'Bohdan Mushkevych'

import os

from settings import settings
from system.data_logging import Logger
from system.decorator import current_process_aware, singleton
from db.model.process_context_entry import ProcessContextEntry

TYPE_ALERT = 'type_alert'
TYPE_HORIZONTAL_AGGREGATOR = 'type_horizontal'
TYPE_VERTICAL_AGGREGATOR = 'type_vertical'
TYPE_GARBAGE_COLLECTOR = 'type_gc'

PROCESS_SCHEDULER = 'Scheduler'
PROCESS_SUPERVISOR = 'Supervisor'
PROCESS_STREAM_GEN = 'EventStreamGenerator'
PROCESS_SESSION_WORKER_00 = 'SingleSessionWorker_00'
PROCESS_SESSION_WORKER_01 = 'SingleSessionWorker_01'
PROCESS_GC = 'GarbageCollectorWorker'
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

_TOKEN_SCHEDULER = 'scheduler'
_TOKEN_SUPERVISOR = 'supervisor'
_TOKEN_STREAM = 'stream'
_TOKEN_SESSION = 'session'
_TOKEN_GC = 'gc'
_TOKEN_SITE = 'site'
_TOKEN_CLIENT = 'client'
_TOKEN_ALERT = 'alert'

_ROUTING_PREFIX = 'routing_'
_QUEUE_PREFIX = 'queue_'


def _process_context_entry(process_name,
                           classname,
                           token,
                           time_qualifier,
                           exchange,
                           arguments=None,
                           queue=None,
                           routing=None,
                           process_type=None,
                           source=None,
                           sink=None,
                           pid_file=None,
                           log_file=None):
    """ forms process context entry """
    if queue is None:
        queue = _QUEUE_PREFIX + token + time_qualifier
    if routing is None:
        routing = _ROUTING_PREFIX + token + time_qualifier
    if pid_file is None:
        pid_file = token + time_qualifier + '.pid'
    if log_file is None:
        log_file = token + time_qualifier + '.log'

    process_entry = ProcessContextEntry()
    process_entry.process_name = process_name
    process_entry.classname = classname
    process_entry.token = token
    process_entry.source = source
    process_entry.sink = sink
    process_entry.mq_queue = queue
    process_entry.mq_routing_key = routing
    process_entry.mq_exchange = exchange
    process_entry.arguments = arguments
    process_entry.time_qualifier = time_qualifier
    process_entry.process_type = process_type
    process_entry.log_filename = log_file
    process_entry.pid_filename = pid_file
    return process_entry


@singleton
class ProcessContext(object):
    __CURRENT_PROCESS_TAG = '__CURRENT_PROCESS'

    QUALIFIER_REAL_TIME = '_real_time'
    QUALIFIER_BY_SCHEDULE = '_by_schedule'
    QUALIFIER_HOURLY = '_hourly'
    QUALIFIER_DAILY = '_daily'
    QUALIFIER_MONTHLY = '_monthly'
    QUALIFIER_YEARLY = '_yearly'

    logger_pool = dict()

    PROCESS_CONTEXT = dict()

    def __init__(self):
        super(ProcessContext, self).__init__()

    @classmethod
    def put_context_entry(cls, context_entry):
        assert isinstance(context_entry, ProcessContextEntry)
        cls.PROCESS_CONTEXT[context_entry.process_name] = context_entry

    @classmethod
    @current_process_aware
    def get_context_entry(cls, process_name=None):
        """ method returns dictionary of strings, preset
        source collection, target collection, queue name, exchange, routing, etc"""
        return cls.PROCESS_CONTEXT[process_name]

    @classmethod
    def set_current_process(cls, process_name):
        if ProcessContext.__CURRENT_PROCESS_TAG in cls.__dict__:
            raise AttributeError('Current process %s is already set'
                                 % cls.__dict__[ProcessContext.__CURRENT_PROCESS_TAG])
        cls.__dict__[ProcessContext.__CURRENT_PROCESS_TAG] = process_name

    @classmethod
    def get_current_process(cls):
        if ProcessContext.__CURRENT_PROCESS_TAG not in cls.__dict__:
            raise AttributeError('Current process is not yet set')
        return cls.__dict__[ProcessContext.__CURRENT_PROCESS_TAG]

    @classmethod
    @current_process_aware
    def create_pid_file(cls, process_name=None):
        """ creates pid file and writes os.pid() in there """
        pid_filename = cls.get_pid_filename(process_name)
        try:
            pid_file = open(pid_filename, mode='w')
            pid_file.write(str(os.getpid()))
        except Exception as e:
            cls.get_logger(process_name).error('Unable to create pid file at: %s, because of: %r' % (pid_filename, e))

    @classmethod
    @current_process_aware
    def remove_pid_file(cls, process_name=None):
        """ removes pid file """
        pid_filename = cls.get_pid_filename(process_name)
        try:
            os.remove(pid_filename)
            cls.get_logger(process_name).info('Removed pid file at: %s' % pid_filename)
        except Exception as e:
            cls.get_logger(process_name).error('Unable to remove pid file at: %s, because of: %r' % (pid_filename, e))

    @classmethod
    @current_process_aware
    def get_logger(cls, process_name=None):
        """ method returns initiated logger"""
        if process_name not in cls.logger_pool:
            file_name = cls.get_log_filename(process_name)
            tag = cls.get_log_tag(process_name)
            cls.logger_pool[process_name] = Logger(file_name, tag)
        return cls.logger_pool[process_name].get_logger()

    @classmethod
    @current_process_aware
    def get_pid_filename(cls, process_name=None):
        """method returns path for the PID FILENAME """
        return settings['pid_directory'] + cls.PROCESS_CONTEXT[process_name].pid_filename

    @classmethod
    @current_process_aware
    def get_classname(cls, process_name=None):
        """ method returns fully qualified classname of the instance running as process"""
        return cls.PROCESS_CONTEXT[process_name].classname

    @classmethod
    @current_process_aware
    def get_log_filename(cls, process_name=None):
        """method returns path for the Log filename"""
        return settings['log_directory'] + cls.PROCESS_CONTEXT[process_name].log_filename

    @classmethod
    @current_process_aware
    def get_log_tag(cls, process_name=None):
        """method returns tag that all logging messages will be marked with"""
        return cls.PROCESS_CONTEXT[process_name].log_tag

    @classmethod
    @current_process_aware
    def get_time_qualifier(cls, process_name=None):
        """ method returns worker/aggregator time scale (like daily or yearly)"""
        return cls.PROCESS_CONTEXT[process_name].time_qualifier

    @classmethod
    @current_process_aware
    def get_routing(cls, process_name=None):
        """ method returns routing; it is used to segregate traffic within the queue
        for instance: routing_hourly for hourly reports, while routing_yearly for yearly reports"""
        return cls.PROCESS_CONTEXT[process_name].mq_routing_key

    @classmethod
    @current_process_aware
    def get_exchange(cls, process_name=None):
        """ method returns exchange for this classname.
        Exchange is a component that sits between queue and the publisher"""
        return cls.PROCESS_CONTEXT[process_name].mq_exchange

    @classmethod
    @current_process_aware
    def get_queue(cls, process_name=None):
        """ method returns queue that is applicable for the worker/aggregator, specified by classname"""
        return cls.PROCESS_CONTEXT[process_name].mq_queue

    @classmethod
    @current_process_aware
    def get_sink(cls, process_name=None):
        """ method returns name of the data sink, as specified for the process"""
        return cls.PROCESS_CONTEXT[process_name].sink

    @classmethod
    @current_process_aware
    def get_source(cls, process_name=None):
        """ method returns name of the data source, as specified for the process"""
        return cls.PROCESS_CONTEXT[process_name].source

    @classmethod
    @current_process_aware
    def get_process_type(cls, process_name=None):
        """ method returns process type
        Supported types are listed in process_context starting with TYPE_ prefix and are enumerated in
        scheduler.start() method"""
        return cls.PROCESS_CONTEXT[process_name].process_type


if __name__ == '__main__':
    pass
