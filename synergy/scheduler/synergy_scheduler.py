from synergy.db.model import scheduler_freerun_entry, scheduler_managed_entry

__author__ = 'Bohdan Mushkevych'

from datetime import datetime, timedelta
from threading import Lock

from amqp import AMQPError

from synergy.mq.flopsy import PublishersPool
from synergy.mx.synergy_mx import MX
from synergy.db.model.worker_mq_request import WorkerMqRequest
from synergy.db.dao.scheduler_managed_entry_dao import SchedulerManagedEntryDao
from synergy.db.dao.scheduler_freerun_entry_dao import SchedulerFreerunEntryDao
from synergy.system import time_helper
from synergy.conf.process_context import ProcessContext
from synergy.system.decorator import with_reconnect, thread_safe
from synergy.system.synergy_process import SynergyProcess
from synergy.system.event_clock import parse_time_trigger_string
from synergy.scheduler.scheduler_constants import *
from synergy.scheduler.continuous_pipeline import ContinuousPipeline
from synergy.scheduler.dicrete_pipeline import DiscretePipeline
from synergy.scheduler.simplified_dicrete_pipeline import SimplifiedDiscretePipeline
from synergy.scheduler.timetable import Timetable


class Scheduler(SynergyProcess):
    """ Scheduler encapsulate logic for handling task pipelines """

    def __init__(self, process_name):
        super(Scheduler, self).__init__(process_name)
        self.lock = Lock()
        self.logger.info('Starting %s' % self.process_name)
        self.publishers = PublishersPool(self.logger)
        self.managed_handlers = dict()
        self.freerun_handlers = dict()
        self.timetable = Timetable(self.logger)
        self.pipelines = self._construct_pipelines()

        self.se_managed_dao = SchedulerManagedEntryDao(self.logger)
        self.se_freerun_dao = SchedulerFreerunEntryDao(self.logger)
        self.mx = None
        self.logger.info('Started %s' % self.process_name)

    def __del__(self):
        for handler in self.managed_handlers:
            handler.cancel()
        self.managed_handlers.clear()
        super(Scheduler, self).__del__()

    def _log_message(self, level, process_name, timetable_record, msg):
        """ method performs logging into log file, Tree node and the Job instance"""
        self.timetable.add_log_entry(process_name, timetable_record, datetime.utcnow(), msg)
        self.logger.log(level, msg)

    def _construct_pipelines(self):
        """ :return: dict in format <state_machine_common_name: instance_of_the_state_machine> """
        pipelines = dict()
        for pipe in [ContinuousPipeline(self.logger, self.timetable),
                     DiscretePipeline(self.logger, self.timetable),
                     SimplifiedDiscretePipeline(self.logger, self.timetable)]:
            pipelines[pipe.name] = pipe
        return pipelines

    def _construct_handler(self, key, trigger_time, function, parameters):
        """ method returns either:
         - alarm clock of type EventClock, when <schedule> is in format 'at HH:MM, ..., HH:MM'
         - repeat timer of type RepeatTimer, when <schedule> is in format 'every seconds'
         On trigger event this module triggers call_back function with arguments (args, kwargs)
        """
        parsed_trigger_time, timer_klass = parse_time_trigger_string(trigger_time)
        timer_instance = timer_klass(parsed_trigger_time, function, args=parameters)
        self.logger.info('Created %s for %r with schedule %r' % (timer_klass.__name__, key, trigger_time))
        return timer_instance

    def _activate_handler(self, scheduler_entry_obj, process_name, entry_name, function, handler_type):
        """ method parses scheduler_entry_obj and creates a timer_handler out of it
         timer_handler is enlisted to either :self.freerun_handlers or :self.managed_handlers
         timer_handler is started, unless it is marked as STATE_OFF """
        trigger_time = scheduler_entry_obj.trigger_time
        is_active = scheduler_entry_obj.state == scheduler_managed_entry.STATE_ON

        if handler_type == TYPE_MANAGED:
            handler_key = process_name
            arguments = [handler_key, scheduler_entry_obj, handler_type]
            handler = self._construct_handler(handler_key, trigger_time, function, arguments)
            self.managed_handlers[handler_key] = handler
        elif handler_type == TYPE_FREERUN:
            handler_key = (process_name, entry_name)
            arguments = [handler_key, scheduler_entry_obj, handler_type]
            handler = self._construct_handler(handler_key, trigger_time, function, arguments)
            self.freerun_handlers[handler_key] = handler
        else:
            self.logger.error('Process/Handler type %s is not known to the system. Skipping it.' % handler_type)
            return

        if is_active:
            handler.start()
            self.logger.info('Started scheduler thread for %s:%r.' % (handler_type, handler_key))
        else:
            self.logger.info('Handler for %s:%r registered in Scheduler. Idle until activated.'
                             % (handler_type, handler_key))

    # **************** Scheduler Methods ************************
    def _load_managed_entries(self):
        """ loads scheduler managed entries. no start-up procedures are performed """
        scheduler_entries = self.se_managed_dao.get_all()
        for scheduler_entry_obj in scheduler_entries:
            process_name = scheduler_entry_obj.process_name
            if scheduler_entry_obj.process_name not in ProcessContext.CONTEXT:
                self.logger.error('Process %r is not known to the system. Skipping it.' % process_name)
                continue

            process_type = ProcessContext.get_process_type(process_name)
            if process_type in [TYPE_BLOCKING_DEPENDENCIES, TYPE_BLOCKING_CHILDREN, TYPE_MANAGED]:
                function = self.fire_managed_worker
                handler_type = TYPE_MANAGED
            elif process_type == TYPE_GARBAGE_COLLECTOR:
                function = self.fire_garbage_collector
                handler_type = TYPE_MANAGED
            elif process_type == TYPE_FREERUN:
                self.logger.error('TYPE_FREERUN process %s was found in scheduler_managed_entry table. '
                                  'Move the process to the scheduler_freerun_entry table. Skipping the process.'
                                  % process_type)
                continue
            else:
                self.logger.error('Process type %s is not known to the system. Skipping it.' % process_type)
                continue

            self._activate_handler(scheduler_entry_obj, process_name, 'NA', function, handler_type)

    def _load_freerun_entries(self):
        """ reads scheduler managed entries and starts their timers to trigger events """
        scheduler_entries = self.se_freerun_dao.get_all()
        for scheduler_entry_obj in scheduler_entries:
            self._activate_handler(scheduler_entry_obj, scheduler_entry_obj.process_name,
                                   scheduler_entry_obj.entry_name, self.fire_freerun_worker, TYPE_FREERUN)

    @with_reconnect
    def start(self, *_):
        """ reading scheduler configurations and starting timers to trigger events """
        try:
            self._load_managed_entries()
        except LookupError as e:
            self.logger.warn('DB Lookup: %s' % str(e))

        try:
            self._load_freerun_entries()
        except LookupError as e:
            self.logger.warn('DB Lookup: %s' % str(e))

        # as Scheduler is now initialized and running - we can safely start its MX
        self.mx = MX(self)
        self.mx.start_mx_thread()

    @thread_safe
    def fire_managed_worker(self, *args):
        """requests vertical aggregator (hourly site, daily variant, etc) to start up"""
        try:
            process_name = args[0]
            scheduler_entry_obj = args[1]
            self.logger.info('%s {' % process_name)

            timetable_record = self.timetable.get_next_job_record(process_name)
            pipeline = self.pipelines[scheduler_entry_obj.state_machine_name]

            run_on_active_timeperiod = ProcessContext.run_on_active_timeperiod(scheduler_entry_obj.process_name)
            if not run_on_active_timeperiod:
                time_qualifier = ProcessContext.get_time_qualifier(process_name)
                incremented_timeperiod = time_helper.increment_timeperiod(time_qualifier, timetable_record.timeperiod)
                dt_record_timestamp = time_helper.synergy_to_datetime(time_qualifier, incremented_timeperiod)
                dt_record_timestamp += timedelta(minutes=LAG_5_MINUTES)

                if datetime.utcnow() <= dt_record_timestamp:
                    self.logger.info('Timetable record %s for timeperiod %s will not be triggered until %s.'
                                     % (timetable_record.document['_id'],
                                        timetable_record.timeperiod,
                                        dt_record_timestamp.strftime('%Y-%m-%d %H:%M:%S')))
                    return

            process_type = ProcessContext.get_process_type(scheduler_entry_obj.process_name)
            if process_type == TYPE_BLOCKING_DEPENDENCIES:
                pipeline.manage_pipeline_with_blocking_dependencies(process_name, timetable_record)
            elif process_type == TYPE_BLOCKING_CHILDREN:
                pipeline.manage_pipeline_with_blocking_children(process_name, timetable_record)
            elif process_type == TYPE_MANAGED:
                pipeline.manage_pipeline_for_process(process_name, timetable_record)

        except (AMQPError, IOError) as e:
            self.logger.error('AMQPError: %s' % str(e), exc_info=True)
            self.publishers.reset_all(suppress_logging=True)
        except Exception as e:
            self.logger.error('Exception: %s' % str(e), exc_info=True)
        finally:
            self.logger.info('}')

    @thread_safe
    def fire_freerun_worker(self, *args):
        """fires free-run worker with no dependencies and history to track"""
        try:
            process_name, entry_name = args[0]
            scheduler_entry_obj = args[1]
            self.logger.info('%s {' % process_name)

            mq_request = WorkerMqRequest()
            mq_request.process_name = scheduler_entry_obj.process_name
            if isinstance(scheduler_entry_obj, scheduler_freerun_entry.SchedulerFreerunEntry):
                mq_request.entry_name = scheduler_entry_obj.entry_name
                mq_request.entry_arguments = scheduler_entry_obj.arguments

            publisher = self.publishers.get(process_name)
            publisher.publish(mq_request.document)
            publisher.release()

            self.logger.info('Published trigger for %s::%s' % (process_name, entry_name))
        except (AMQPError, IOError) as e:
            self.logger.error('AMQPError: %s' % str(e), exc_info=True)
            self.publishers.reset_all(suppress_logging=True)
        except Exception as e:
            self.logger.error('fire_freerun_worker: %s' % str(e))
        finally:
            self.logger.info('}')

    @thread_safe
    def fire_garbage_collector(self, *args):
        """fires garbage collector to re-trigger invalid unit_of_work"""
        try:
            process_name = args[0]
            scheduler_entry_obj = args[1]
            assert isinstance(scheduler_entry_obj, scheduler_managed_entry.SchedulerManagedEntry)
            self.logger.info('%s {' % process_name)

            mq_request = WorkerMqRequest()
            mq_request.process_name = process_name

            publisher = self.publishers.get(process_name)
            publisher.publish(mq_request.document)
            publisher.release()
            self.logger.info('Published trigger for %s' % process_name)

            self.logger.info('Starting timetable housekeeping...')
            self.timetable.build_trees()
            self.timetable.validate()
            self.logger.info('Timetable housekeeping complete.')
        except (AMQPError, IOError) as e:
            self.logger.error('AMQPError: %s' % str(e), exc_info=True)
            self.publishers.reset_all(suppress_logging=True)
        except Exception as e:
            self.logger.error('fire_garbage_collector: %s' % str(e))
        finally:
            self.logger.info('}')


if __name__ == '__main__':
    from synergy.scheduler.scheduler_constants import PROCESS_SCHEDULER

    source = Scheduler(PROCESS_SCHEDULER)
    source.start()
