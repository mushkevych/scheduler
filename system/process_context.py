"""
Created on 2011-03-11

@author: Bohdan Mushkevych
"""
import os

from system.data_logging import Logger
from settings import settings

PROCESS_STREAM_GEN = 'EventStreamGenerator'
PROCESS_SCHEDULER = 'Scheduler'
PROCESS_SUPERVISOR = 'Supervisor'
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
    # }

    QUEUE_RAW_DATA = 'queue_raw_data'
    QUEUE_VERTICAL_SITE = 'queue_vertical_site'
    QUEUE_HORIZONTAL_CLIENT = 'queue_horizontal_client'
    QUEUE_ALERT = 'queue_alert'
    QUEUE_GARBAGE_COLLECTOR = 'queue_garbage_collector'

    QUALIFIER_REAL_TIME = 'real_time'
    QUALIFIER_BY_SCHEDULE = 'by_schedule'
    QUALIFIER_HOURLY = '_hourly'
    QUALIFIER_DAILY = '_daily'
    QUALIFIER_MONTHLY = '_monthly'
    QUALIFIER_YEARLY = '_yearly'

    EXCHANGE_RAW_DATA = 'exchange_raw_data'
    EXCHANGE_VERTICAL = 'exchange_vertical'
    EXCHANGE_HORIZONTAL = 'exchange_horizontal'
    EXCHANGE_ALERT = 'exchange_alert'
    EXCHANGE_UTILS = 'exchange_utils'

    ROUTING_IRRELEVANT = 'routing_irrelevant'
    ROUTING_GC = 'routing_gc'

    ROUTING_VERTICAL_SITE = 'routing_vertical_site'
    ROUTING_HORIZONTAL_CLIENT = 'routing_horizontal_client'
    ROUTING_ALERT = 'routing_alert'

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
    VOID = 'VOID'

    logger_pool = dict()

    PROCESS_CONTEXT = {
        PROCESS_SITE_DAILY: {_PID_FILENAME: settings['pid_directory'] + 'site_daily_aggregator.pid',
                             _CLASSNAME: 'workers.hadoop_aggregator_driver.HadoopAggregatorDriver',
                             _LOG_FILENAME: settings['log_directory'] + 'site_daily_aggregator.log',
                             _LOG_TAG: 'daily_site',
                             _SOURCE_COLLECTION: VOID,
                             _TARGET_COLLECTION: VOID,
                             _MQ_QUEUE: QUEUE_VERTICAL_SITE + QUALIFIER_DAILY,
                             _MQ_EXCHANGE: EXCHANGE_VERTICAL,
                             _MQ_ROUTING_KEY: ROUTING_VERTICAL_SITE + QUALIFIER_DAILY,
                             _TIME_QUALIFIER: QUALIFIER_DAILY
        },
        PROCESS_SITE_HOURLY: {_PID_FILENAME: settings['pid_directory'] + 'site_hourly_aggregator.pid',
                              _CLASSNAME: 'workers.site_hourly_aggregator.SiteHourlyAggregator',
                              _LOG_FILENAME: settings['log_directory'] + 'site_hourly_aggregator.log',
                              _LOG_TAG: 'hourly_site',
                              _SOURCE_COLLECTION: 'single_session_collection',
                              _TARGET_COLLECTION: VOID,
                              _MQ_QUEUE: QUEUE_VERTICAL_SITE + QUALIFIER_HOURLY,
                              _MQ_EXCHANGE: EXCHANGE_VERTICAL,
                              _MQ_ROUTING_KEY: ROUTING_VERTICAL_SITE + QUALIFIER_HOURLY,
                              _TIME_QUALIFIER: QUALIFIER_HOURLY
        },
        PROCESS_SITE_MONTHLY: {_PID_FILENAME: settings['pid_directory'] + 'site_monthly_aggregator.pid',
                               _CLASSNAME: 'workers.hadoop_aggregator_driver.HadoopAggregatorDriver',
                               _LOG_FILENAME: settings['log_directory'] + 'site_monthly_aggregator.log',
                               _LOG_TAG: 'monthly_site',
                               _SOURCE_COLLECTION: VOID,
                               _TARGET_COLLECTION: VOID,
                               _MQ_QUEUE: QUEUE_VERTICAL_SITE + QUALIFIER_MONTHLY,
                               _MQ_EXCHANGE: EXCHANGE_VERTICAL,
                               _MQ_ROUTING_KEY: ROUTING_VERTICAL_SITE + QUALIFIER_MONTHLY,
                               _TIME_QUALIFIER: QUALIFIER_MONTHLY
        },
        PROCESS_SITE_YEARLY: {_PID_FILENAME: settings['pid_directory'] + 'site_yearly_aggregator.pid',
                              _CLASSNAME: 'workers.hadoop_aggregator_driver.HadoopAggregatorDriver',
                              _LOG_FILENAME: settings['log_directory'] + 'site_yearly_aggregator.log',
                              _LOG_TAG: 'yearly_site',
                              _SOURCE_COLLECTION: VOID,
                              _TARGET_COLLECTION: VOID,
                              _MQ_QUEUE: QUEUE_VERTICAL_SITE + QUALIFIER_YEARLY,
                              _MQ_EXCHANGE: EXCHANGE_VERTICAL,
                              _MQ_ROUTING_KEY: ROUTING_VERTICAL_SITE + QUALIFIER_YEARLY,
                              _TIME_QUALIFIER: QUALIFIER_YEARLY
        },
        PROCESS_GC: {_PID_FILENAME: settings['pid_directory'] + 'garbage_collector.pid',
                     _CLASSNAME: 'workers.garbage_collector_worker.GarbageCollectorWorker',
                     _LOG_FILENAME: settings['log_directory'] + 'garbage_collector.log',
                     _LOG_TAG: 'GC',
                     _SOURCE_COLLECTION: 'units_of_work_collection',
                     _TARGET_COLLECTION: 'units_of_work_collection',
                     _MQ_QUEUE: QUEUE_GARBAGE_COLLECTOR,
                     _MQ_EXCHANGE: EXCHANGE_UTILS,
                     _MQ_ROUTING_KEY: ROUTING_GC,
                     _TIME_QUALIFIER: QUALIFIER_BY_SCHEDULE
        },
        PROCESS_SESSION_WORKER_00: {_PID_FILENAME: settings['pid_directory'] + 'session_worker_00.pid',
                                    _CLASSNAME: 'workers.single_session_worker.SingleSessionWorker',
                                    _LOG_FILENAME: settings['log_directory'] + 'session_worker_00.log',
                                    _LOG_TAG: 'session_worker_00',
                                    _SOURCE_COLLECTION: 'single_session_collection',
                                    _TARGET_COLLECTION: 'single_session_collection',
                                    _MQ_QUEUE: QUEUE_RAW_DATA,
                                    _MQ_EXCHANGE: EXCHANGE_RAW_DATA,
                                    _MQ_ROUTING_KEY: ROUTING_IRRELEVANT,
                                    _TIME_QUALIFIER: QUALIFIER_REAL_TIME
        },
        PROCESS_SESSION_WORKER_01: {_PID_FILENAME: settings['pid_directory'] + 'session_worker_01.pid',
                                    _CLASSNAME: 'workers.single_session_worker.SingleSessionWorker',
                                    _LOG_FILENAME: settings['log_directory'] + 'session_worker_01.log',
                                    _LOG_TAG: 'session_worker_01',
                                    _SOURCE_COLLECTION: 'single_session_collection',
                                    _TARGET_COLLECTION: 'single_session_collection',
                                    _MQ_QUEUE: QUEUE_RAW_DATA,
                                    _MQ_EXCHANGE: EXCHANGE_RAW_DATA,
                                    _MQ_ROUTING_KEY: ROUTING_IRRELEVANT,
                                    _TIME_QUALIFIER: QUALIFIER_REAL_TIME
        },
        PROCESS_SCHEDULER: {_PID_FILENAME: settings['pid_directory'] + 'scheduler.pid',
                            _CLASSNAME: 'scheduler.scheduler.Scheduler',
                            _LOG_FILENAME: settings['log_directory'] + 'scheduler.log',
                            _LOG_TAG: 'scheduler',
                            _SOURCE_COLLECTION: VOID,
                            _TARGET_COLLECTION: VOID,
                            _MQ_QUEUE: '',
                            _MQ_EXCHANGE: '',
                            _MQ_ROUTING_KEY: '',
                            _TIME_QUALIFIER: ''
        },
        PROCESS_SUPERVISOR: {_PID_FILENAME: settings['pid_directory'] + 'supervisor.pid',
                             _CLASSNAME: 'supervisor.supervisor.Supervisor',
                             _LOG_FILENAME: settings['log_directory'] + 'supervisor.log',
                             _LOG_TAG: 'supervisor',
                             _SOURCE_COLLECTION: VOID,
                             _TARGET_COLLECTION: VOID,
                             _MQ_QUEUE: '',
                             _MQ_EXCHANGE: '',
                             _MQ_ROUTING_KEY: '',
                             _TIME_QUALIFIER: ''
        },
        PROCESS_STREAM_GEN: {_PID_FILENAME: settings['pid_directory'] + 'event_generator.pid',
                             _CLASSNAME: 'event_stream_generator.event_stream_generator.EventStreamGenerator',
                             _LOG_FILENAME: settings['log_directory'] + 'event_generator.log',
                             _LOG_TAG: 'event_generator',
                             _SOURCE_COLLECTION: VOID,
                             _TARGET_COLLECTION: VOID,
                             _MQ_QUEUE: QUEUE_RAW_DATA,
                             _MQ_EXCHANGE: EXCHANGE_RAW_DATA,
                             _MQ_ROUTING_KEY: ROUTING_IRRELEVANT,
                             _TIME_QUALIFIER: QUALIFIER_REAL_TIME
        },
        PROCESS_CLIENT_DAILY: {_PID_FILENAME: settings['pid_directory'] + 'client_daily.pid',
                               _CLASSNAME: 'workers.hadoop_aggregator_driver.HadoopAggregatorDriver',
                               _LOG_FILENAME: settings['log_directory'] + 'client_daily.log',
                               _LOG_TAG: 'client_daily',
                               _SOURCE_COLLECTION: VOID,
                               _TARGET_COLLECTION: VOID,
                               _MQ_QUEUE: QUEUE_HORIZONTAL_CLIENT + QUALIFIER_DAILY,
                               _MQ_EXCHANGE: EXCHANGE_HORIZONTAL,
                               _MQ_ROUTING_KEY: ROUTING_HORIZONTAL_CLIENT + QUALIFIER_DAILY,
                               _TIME_QUALIFIER: QUALIFIER_DAILY
        },
        PROCESS_CLIENT_MONTHLY: {_PID_FILENAME: settings['pid_directory'] + 'client_monthly.pid',
                                 _CLASSNAME: 'workers.hadoop_aggregator_driver.HadoopAggregatorDriver',
                                 _LOG_FILENAME: settings['log_directory'] + 'client_monthly.log',
                                 _LOG_TAG: 'client_monthly',
                                 _SOURCE_COLLECTION: VOID,
                                 _TARGET_COLLECTION: VOID,
                                 _MQ_QUEUE: QUEUE_HORIZONTAL_CLIENT + QUALIFIER_MONTHLY,
                                 _MQ_EXCHANGE: EXCHANGE_HORIZONTAL,
                                 _MQ_ROUTING_KEY: ROUTING_HORIZONTAL_CLIENT + QUALIFIER_MONTHLY,
                                 _TIME_QUALIFIER: QUALIFIER_MONTHLY
        },
        PROCESS_CLIENT_YEARLY: {_PID_FILENAME: settings['pid_directory'] + 'client_yearly.pid',
                                _CLASSNAME: 'workers.hadoop_aggregator_driver.HadoopAggregatorDriver',
                                _LOG_FILENAME: settings['log_directory'] + 'client_yearly.log',
                                _LOG_TAG: 'client_yearly',
                                _SOURCE_COLLECTION: VOID,
                                _TARGET_COLLECTION: VOID,
                                _MQ_QUEUE: QUEUE_HORIZONTAL_CLIENT + QUALIFIER_YEARLY,
                                _MQ_EXCHANGE: EXCHANGE_HORIZONTAL,
                                _MQ_ROUTING_KEY: ROUTING_HORIZONTAL_CLIENT + QUALIFIER_YEARLY,
                                _TIME_QUALIFIER: QUALIFIER_YEARLY
        },
        PROCESS_ALERT_DAILY: {_PID_FILENAME: settings['pid_directory'] + 'alert_daily.pid',
                              _CLASSNAME: 'workers.hadoop_aggregator_driver.HadoopAggregatorDriver',
                              _LOG_FILENAME: settings['log_directory'] + 'alert_daily.log',
                              _LOG_TAG: 'alert_daily',
                              _SOURCE_COLLECTION: VOID,
                              _TARGET_COLLECTION: VOID,
                              _MQ_QUEUE: QUEUE_ALERT + QUALIFIER_DAILY,
                              _MQ_EXCHANGE: EXCHANGE_ALERT,
                              _MQ_ROUTING_KEY: ROUTING_ALERT + QUALIFIER_DAILY,
                              _TIME_QUALIFIER: QUALIFIER_DAILY
        },
        'TestAggregator': {_PID_FILENAME: '',
                           _CLASSNAME: '',
                           _LOG_FILENAME: settings['log_directory'] + 'synergy_tests.log',
                           _LOG_TAG: 'test',
                           _SOURCE_COLLECTION: VOID,
                           _TARGET_COLLECTION: VOID,
                           _MQ_QUEUE: '',
                           _MQ_EXCHANGE: '',
                           _MQ_ROUTING_KEY: '',
                           _TIME_QUALIFIER: ''
        }
    }

    @classmethod
    def create_pid_file(cls, process_name):
        """ creates pid file and writes os.pid() in there """
        pid_filename = cls.get_pid_filename(process_name)
        try:
            pid_file = open(pid_filename, mode='w')
            pid_file.write(str(os.getpid()))
        except Exception as e:
            cls.get_logger(process_name).error('Unable to create pid file at: %s, because of: %r' % (pid_filename, e))

    @classmethod
    def remove_pid_file(cls, process_name):
        """ removes pid file """
        pid_filename = cls.get_pid_filename(process_name)
        try:
            os.remove(pid_filename)
            cls.get_logger(process_name).info('Removed pid file at: %s' % pid_filename)
        except Exception as e:
            cls.get_logger(process_name).error('Unable to remove pid file at: %s, because of: %r' % (pid_filename, e))

    @classmethod
    def get_logger(cls, process_name):
        """ method returns initiated logger"""
        if process_name not in cls.logger_pool:
            file_name = cls.get_log_filename(process_name)
            tag = cls.get_log_tag(process_name)
            cls.logger_pool[process_name] = Logger(file_name, tag)
        return cls.logger_pool[process_name].get_logger()

    @classmethod
    def get_record(cls, process_name):
        """ method returns dictionary of strings, preset
        source collection, target collection, queue name, exchange, routing, etc"""
        return cls.PROCESS_CONTEXT[process_name]

    @classmethod
    def get_pid_filename(cls, process_name):
        """method returns path for the PID FILENAME """
        return cls.PROCESS_CONTEXT[process_name][cls._PID_FILENAME]

    @classmethod
    def get_classname(cls, process_name):
        """ method returns fully qualified classname of the instance running as process"""
        return cls.PROCESS_CONTEXT[process_name][cls._CLASSNAME]

    @classmethod
    def get_log_filename(cls, process_name):
        """method returns path for the Log filename"""
        return cls.PROCESS_CONTEXT[process_name][cls._LOG_FILENAME]

    @classmethod
    def get_log_tag(cls, process_name):
        """method returns tag that all logging messages will be marked with"""
        return cls.PROCESS_CONTEXT[process_name][cls._LOG_TAG]

    @classmethod
    def get_time_qualifier(cls, process_name):
        """ method returns worker/aggregator time scale (like daily or yearly)"""
        return cls.PROCESS_CONTEXT[process_name][cls._TIME_QUALIFIER]

    @classmethod
    def get_routing(cls, process_name):
        """ method returns routing; it is used to segregate traffic within the queue
        for instance: routing_hourly for hourly reports, while routing_yearly for yearly reports"""
        return cls.PROCESS_CONTEXT[process_name][cls._MQ_ROUTING_KEY]

    @classmethod
    def get_exchange(cls, process_name):
        """ method returns exchange for this classname.
        Exchange is a component that sits between queue and the publisher"""
        return cls.PROCESS_CONTEXT[process_name][cls._MQ_EXCHANGE]

    @classmethod
    def get_queue(cls, process_name):
        """ method returns queue that is applicable for the worker/aggregator, specified by classname"""
        return cls.PROCESS_CONTEXT[process_name][cls._MQ_QUEUE]

    @classmethod
    def get_target_collection(cls, process_name):
        """ method returns target collection - the one where aggregated data will be placed in """
        return cls.PROCESS_CONTEXT[process_name][cls._TARGET_COLLECTION]

    @classmethod
    def get_source_collection(cls, process_name):
        """ method returns source collection - the one where data is taken from for analysis"""
        return cls.PROCESS_CONTEXT[process_name][cls._SOURCE_COLLECTION]


if __name__ == '__main__':
    pass