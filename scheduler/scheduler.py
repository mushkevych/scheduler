"""
Created on 2011-02-07

@author: Bohdan Mushkevych
"""

from datetime import datetime
from threading import Lock
from amqplib.client_0_8 import AMQPException

from mx.mx import MX
from flopsy.flopsy import PublishersPool
from system.synergy_process import SynergyProcess
from system.collection_context import CollectionContext, with_reconnect
from system.collection_context import COLLECTION_SCHEDULER_CONFIGURATION
from system.repeat_timer import RepeatTimer
from system.process_context import *

from hadoop_pipeline import HadoopPipeline
from regular_pipeline import RegularPipeline
from scheduler_configuration_collection import SchedulerConfigurationCollection
from time_table import TimeTable


class Scheduler(SynergyProcess):
    """ Scheduler encapsulate logic for starting the aggregators/alerts/other readers """

    def __init__(self, process_name):
        super(Scheduler, self).__init__(process_name)
        self.logger.info('Starting %s' % self.process_name)
        self.publishers = PublishersPool(self.logger)
        self.thread_handlers = dict()
        self.lock = Lock()
        self.timetable = TimeTable(self.logger)
        self.regular_pipeline = RegularPipeline(self, self.timetable)
        self.hadoop_pipeline = HadoopPipeline(self, self.timetable)
        self.mx = MX(self)
        self.logger.info('Started %s' % self.process_name)


    def __del__(self):
        for handler in self.thread_handlers:
            handler.cancel()
        self.thread_handlers.clear()
        super(Scheduler, self).__del__()


    def _log_message(self, level, process_name, time_record, msg):
        """ method performs logging into log file and TimeTable node"""
        self.timetable.add_log_entry(process_name, time_record, datetime.utcnow(), msg)
        self.logger.log(level, msg)


    # **************** Scheduler Methods ************************
    @with_reconnect
    def start(self):
        """ reading scheduler configurations and starting timers to trigger events """
        collection = CollectionContext.get_collection(self.logger, COLLECTION_SCHEDULER_CONFIGURATION)
        cursor = collection.find({})
        if cursor.count() == 0:
            raise LookupError('MongoDB has no scheduler configuration entries')

        for entry in cursor:
            document = SchedulerConfigurationCollection(entry)
            interval = document.get_interval()
            parameters = [document.get_process_name()]

            if document.get_type() == document.TYPE_ALERT:
                function = self.fire_alert
            elif document.get_type() == document.TYPE_HORIZONTAL_AGGREGATOR:
                function = self.fire_worker
            elif document.get_type() == document.TYPE_VERTICAL_AGGREGATOR:
                function = self.fire_worker
            elif document.get_type() == document.TYPE_GARBAGE_COLLECTOR:
                function = self.fire_garbage_collector
            else:
                self.logger.error('Can not start scheduler for %s since it has no processing function'\
                                    % (document.get_type()))
                continue

            handler = RepeatTimer(interval, function, args=parameters)
            self.thread_handlers[document.get_process_name()] = handler
            handler.start()
            self.logger.info('Started scheduler for %s:%s, triggering every %d seconds'\
                                % (document.get_type(), document.get_process_name(), interval))

        self.mx.start_mx_thread()


    def fire_worker(self, *args):
        """requests vertical aggregator (hourly site, daily variant, etc) to start up"""
        try:
            process_name = args[0]
            self.lock.acquire()
            self.logger.info('%s {' % process_name)
            time_record = self.timetable.get_next_timetable_record(process_name)
            time_qualifier = ProcessContext.get_time_qualifier(process_name)

            if time_qualifier == ProcessContext.QUALIFIER_HOURLY:
                self.regular_pipeline.manage_pipeline_for_process(process_name, time_record)
            else:
                self.hadoop_pipeline.manage_pipeline_for_process(process_name, time_record)

        except (AMQPException, IOError) as e:
            self.logger.error('AMQPException: %s' % str(e), exc_info=True)
            self.publishers.reset_all_publishers(suppress_logging=True)
        except Exception as e:
            self.logger.error('Exception: %s' % str(e), exc_info=True)
        finally:
            self.logger.info('}')
            self.lock.release()


    def fire_alert(self, *args):
        """ Triggers AlertWorker. Makes sure its <dependent on> trees have
            finalized corresponding timeperiods prior to that"""
        try:
            process_name = args[0]
            self.lock.acquire()
            self.logger.info('%s {' % process_name)

            time_record = self.timetable.get_next_timetable_record(process_name)
            self.hadoop_pipeline.manage_pipeline_with_blocking_dependencies(process_name, time_record)
        except (AMQPException, IOError) as e:
            self.logger.error('AMQPException: %s' % str(e), exc_info=True)
            self.publishers.reset_all_publishers(suppress_logging=True)
        except Exception as e:
            self.logger.error('Exception: %s' % str(e), exc_info=True)
        finally:
            self.logger.info('}')
            self.lock.release()


    def fire_garbage_collector(self, *args):
        """fires garbage collector to re-run all invalid records"""
        try:
            process_name = args[0]
            self.lock.acquire()
            self.logger.info('%s {' % process_name)

            self.publishers.get_publisher(process_name).publish({})
            self.logger.info('Publishing trigger for garbage_collector')
            self.timetable.build_tree()
            self.timetable.validate()
            self.logger.info('Validated Timetable for all trees')
        except (AMQPException, IOError) as e:
            self.logger.error('AMQPException: %s' % str(e), exc_info=True)
            self.publishers.reset_all_publishers(suppress_logging=True)
        except Exception as e:
            self.logger.error('fire_garbage_collector: %s' % str(e))
        finally:
            self.logger.info('}')
            self.lock.release()


if __name__ == '__main__':
    from system.process_context import PROCESS_SCHEDULER

    source = Scheduler(PROCESS_SCHEDULER)
    source.start()