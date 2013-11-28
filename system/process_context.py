from system.decorator import current_process_aware

__author__ = 'Bohdan Mushkevych'


import os

from system.data_logging import Logger
from settings import settings

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
_VOID = 'VOID'

_NAME = 'process_name'
_LOG_FILENAME = 'log_filename'
_LOG_TAG = 'log_tag'
_PID_FILENAME = 'pid_filename'
_CLASSNAME = 'classname'
_SOURCE_COLLECTION = 'source_collection'
_TARGET_COLLECTION = 'target_collection'
_MQ_QUEUE = 'mq_queue'
_MQ_EXCHANGE = 'mq_exchange'
_MQ_ROUTING_KEY = 'mq_routing_key'
_TIME_QUALIFIER = 'time_qualifier'
_TYPE = 'type'


def _create_context_entry(process_name,
                          classname,
                          token,
                          time_qualifier,
                          exchange,
                          queue=None,
                          routing=None,
                          process_type=None,
                          source_collection=_VOID,
                          target_collection=_VOID,
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

    return {
        _NAME: process_name,
        _PID_FILENAME: settings['pid_directory'] + pid_file,
        _CLASSNAME: classname,
        _LOG_FILENAME: settings['log_directory'] + log_file,
        _LOG_TAG: token + time_qualifier,
        _SOURCE_COLLECTION: source_collection,
        _TARGET_COLLECTION: target_collection,
        _MQ_QUEUE: queue,
        _MQ_EXCHANGE: exchange,
        _MQ_ROUTING_KEY: routing,
        _TIME_QUALIFIER: time_qualifier,
        _TYPE: process_type
    }


class ProcessContext:
    # process_context format: "process_name": {
    # process_name
    # log_filename
    # log_tag
    # pid_filename
    # full_classname
    # source_collection
    # target_collection
    # mq_queue
    # mq_exchange
    # mq_routing_key
    # time_qualifier
    # type
    # }
    __CURRENT_PROCESS_TAG = '__CURRENT_PROCESS'


    QUEUE_RAW_DATA = 'queue_raw_data'
    ROUTING_IRRELEVANT = 'routing_irrelevant'

    QUALIFIER_REAL_TIME = '_real_time'
    QUALIFIER_BY_SCHEDULE = '_by_schedule'
    QUALIFIER_HOURLY = '_hourly'
    QUALIFIER_DAILY = '_daily'
    QUALIFIER_MONTHLY = '_monthly'
    QUALIFIER_YEARLY = '_yearly'

    EXCHANGE_RAW_DATA = 'exchange_raw_data'
    EXCHANGE_VERTICAL = 'exchange_vertical'
    EXCHANGE_HORIZONTAL = 'exchange_horizontal'
    EXCHANGE_ALERT = 'exchange_alert'
    EXCHANGE_UTILS = 'exchange_utils'

    logger_pool = dict()

    PROCESS_CONTEXT = {
        PROCESS_SITE_DAILY: _create_context_entry(
            process_name=PROCESS_SITE_DAILY,
            classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
            token=_TOKEN_SITE,
            time_qualifier=QUALIFIER_DAILY,
            exchange=EXCHANGE_VERTICAL,
            process_type=TYPE_VERTICAL_AGGREGATOR),

        PROCESS_SITE_HOURLY: _create_context_entry(
            process_name=PROCESS_SITE_HOURLY,
            classname='workers.site_hourly_aggregator.SiteHourlyAggregator.start',
            token=_TOKEN_SITE,
            time_qualifier=QUALIFIER_HOURLY,
            exchange=EXCHANGE_VERTICAL,
            process_type=TYPE_VERTICAL_AGGREGATOR,
            source_collection='single_session'),

        PROCESS_SITE_MONTHLY: _create_context_entry(
            process_name=PROCESS_SITE_MONTHLY,
            classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
            token=_TOKEN_SITE,
            time_qualifier=QUALIFIER_MONTHLY,
            exchange=EXCHANGE_VERTICAL,
            process_type=TYPE_VERTICAL_AGGREGATOR),

        PROCESS_SITE_YEARLY: _create_context_entry(
            process_name=PROCESS_SITE_YEARLY,
            classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
            token=_TOKEN_SITE,
            time_qualifier=QUALIFIER_YEARLY,
            exchange=EXCHANGE_VERTICAL,
            process_type=TYPE_VERTICAL_AGGREGATOR),

        PROCESS_GC: _create_context_entry(
            process_name=PROCESS_GC,
            classname='workers.garbage_collector_worker.GarbageCollectorWorker.start',
            token=_TOKEN_GC,
            time_qualifier=QUALIFIER_BY_SCHEDULE,
            exchange=EXCHANGE_UTILS,
            process_type=TYPE_GARBAGE_COLLECTOR,
            source_collection='units_of_work',
            target_collection='units_of_work'),

        PROCESS_SESSION_WORKER_00: _create_context_entry(
            process_name=PROCESS_SESSION_WORKER_00,
            classname='workers.single_session_worker.SingleSessionWorker.start',
            token=_TOKEN_SESSION,
            time_qualifier=QUALIFIER_REAL_TIME,
            queue=QUEUE_RAW_DATA,
            routing=ROUTING_IRRELEVANT,
            exchange=EXCHANGE_RAW_DATA,
            source_collection='single_session',
            target_collection='single_session',
            pid_file='session_worker_00.pid',
            log_file='session_worker_00.log'),

        PROCESS_SCHEDULER: _create_context_entry(
            process_name=PROCESS_SCHEDULER,
            classname='scheduler.scheduler.Scheduler.start',
            token=_TOKEN_SCHEDULER,
            time_qualifier='',
            queue='',
            routing='',
            exchange=''),

        PROCESS_SUPERVISOR: _create_context_entry(
            process_name=PROCESS_SUPERVISOR,
            classname='supervisor.supervisor.Supervisor.start',
            token=_TOKEN_SUPERVISOR,
            time_qualifier='',
            queue='',
            routing='',
            exchange=''),

        PROCESS_STREAM_GEN: _create_context_entry(
            process_name=PROCESS_STREAM_GEN,
            classname='event_stream_generator.event_stream_generator.EventStreamGenerator.start',
            token=_TOKEN_STREAM,
            time_qualifier=QUALIFIER_REAL_TIME,
            queue=QUEUE_RAW_DATA,
            routing=ROUTING_IRRELEVANT,
            exchange=EXCHANGE_RAW_DATA),

        PROCESS_CLIENT_DAILY: _create_context_entry(
            process_name=PROCESS_CLIENT_DAILY,
            classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
            token=_TOKEN_CLIENT,
            time_qualifier=QUALIFIER_DAILY,
            exchange=EXCHANGE_HORIZONTAL,
            process_type=TYPE_HORIZONTAL_AGGREGATOR),

        PROCESS_CLIENT_MONTHLY: _create_context_entry(
            process_name=PROCESS_CLIENT_MONTHLY,
            classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
            token=_TOKEN_CLIENT,
            time_qualifier=QUALIFIER_MONTHLY,
            exchange=EXCHANGE_HORIZONTAL,
            process_type=TYPE_HORIZONTAL_AGGREGATOR),

        PROCESS_CLIENT_YEARLY: _create_context_entry(
            process_name=PROCESS_CLIENT_YEARLY,
            classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
            token=_TOKEN_CLIENT,
            time_qualifier=QUALIFIER_YEARLY,
            exchange=EXCHANGE_HORIZONTAL,
            process_type=TYPE_HORIZONTAL_AGGREGATOR),

        PROCESS_ALERT_DAILY: _create_context_entry(
            process_name=PROCESS_ALERT_DAILY,
            classname='workers.hadoop_aggregator_driver.HadoopAggregatorDriver.start',
            token=_TOKEN_ALERT,
            time_qualifier=QUALIFIER_DAILY,
            exchange=EXCHANGE_ALERT,
            process_type=TYPE_HORIZONTAL_AGGREGATOR),

        PROCESS_UNIT_TEST: _create_context_entry(
            process_name=PROCESS_UNIT_TEST,
            classname='',
            token='unit_test',
            time_qualifier=QUALIFIER_REAL_TIME,
            routing=ROUTING_IRRELEVANT,
            exchange=EXCHANGE_UTILS),

        PROCESS_LAUNCH_PY: _create_context_entry(
            process_name=PROCESS_LAUNCH_PY,
            classname='',
            token='launch_py',
            time_qualifier=QUALIFIER_REAL_TIME,
            routing=ROUTING_IRRELEVANT,
            exchange=EXCHANGE_UTILS),
    }

    @classmethod
    def set_current_process(cls, process_name):
        if ProcessContext.__CURRENT_PROCESS_TAG in cls.__dict__:
            raise AttributeError('Current process %s is already set' % cls.__dict__[ProcessContext.__CURRENT_PROCESS_TAG])
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
    def get_record(cls, process_name=None):
        """ method returns dictionary of strings, preset
        source collection, target collection, queue name, exchange, routing, etc"""
        return cls.PROCESS_CONTEXT[process_name]

    @classmethod
    @current_process_aware
    def get_pid_filename(cls, process_name=None):
        """method returns path for the PID FILENAME """
        return cls.PROCESS_CONTEXT[process_name][_PID_FILENAME]

    @classmethod
    @current_process_aware
    def get_classname(cls, process_name=None):
        """ method returns fully qualified classname of the instance running as process"""
        return cls.PROCESS_CONTEXT[process_name][_CLASSNAME]

    @classmethod
    @current_process_aware
    def get_log_filename(cls, process_name=None):
        """method returns path for the Log filename"""
        return cls.PROCESS_CONTEXT[process_name][_LOG_FILENAME]

    @classmethod
    @current_process_aware
    def get_log_tag(cls, process_name=None):
        """method returns tag that all logging messages will be marked with"""
        return cls.PROCESS_CONTEXT[process_name][_LOG_TAG]

    @classmethod
    @current_process_aware
    def get_time_qualifier(cls, process_name=None):
        """ method returns worker/aggregator time scale (like daily or yearly)"""
        return cls.PROCESS_CONTEXT[process_name][_TIME_QUALIFIER]

    @classmethod
    @current_process_aware
    def get_routing(cls, process_name=None):
        """ method returns routing; it is used to segregate traffic within the queue
        for instance: routing_hourly for hourly reports, while routing_yearly for yearly reports"""
        return cls.PROCESS_CONTEXT[process_name][_MQ_ROUTING_KEY]

    @classmethod
    @current_process_aware
    def get_exchange(cls, process_name=None):
        """ method returns exchange for this classname.
        Exchange is a component that sits between queue and the publisher"""
        return cls.PROCESS_CONTEXT[process_name][_MQ_EXCHANGE]

    @classmethod
    @current_process_aware
    def get_queue(cls, process_name=None):
        """ method returns queue that is applicable for the worker/aggregator, specified by classname"""
        return cls.PROCESS_CONTEXT[process_name][_MQ_QUEUE]

    @classmethod
    @current_process_aware
    def get_target_collection(cls, process_name=None):
        """ method returns target collection - the one where aggregated data will be placed in """
        return cls.PROCESS_CONTEXT[process_name][_TARGET_COLLECTION]

    @classmethod
    @current_process_aware
    def get_source_collection(cls, process_name=None):
        """ method returns source collection - the one where data is taken from for analysis"""
        return cls.PROCESS_CONTEXT[process_name][_SOURCE_COLLECTION]

    @classmethod
    @current_process_aware
    def get_type(cls, process_name=None):
        """ method returns process type
        Supported types are listed in process_context starting with TYPE_ prefix and are enumerated in
        scheduler.start() method"""
        return cls.PROCESS_CONTEXT[process_name][_TYPE]

if __name__ == '__main__':
    pass