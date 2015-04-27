__author__ = 'Bohdan Mushkevych'

from datetime import datetime, timedelta
from threading import Lock

from amqp import AMQPError

from synergy.conf import context
from synergy.mq.flopsy import PublishersPool
from synergy.mx.synergy_mx import MX
from synergy.db.manager import db_manager
from synergy.db.model.synergy_mq_transmission import SynergyMqTransmission
from synergy.db.model.managed_process_entry import ManagedProcessEntry
from synergy.db.dao.freerun_process_dao import FreerunProcessDao
from synergy.system import time_helper
from synergy.system.decorator import with_reconnect, thread_safe
from synergy.system.synergy_process import SynergyProcess
from synergy.scheduler.status_bus_listener import StatusBusListener
from synergy.scheduler.scheduler_constants import *
from synergy.scheduler.state_machine_continuous import StateMachineContinuous
from synergy.scheduler.state_machine_dicrete import StateMachineDiscrete
from synergy.scheduler.state_machine_simple_dicrete import StateMachineSimpleDiscrete
from synergy.scheduler.state_machine_freerun import StateMachineFreerun
from synergy.scheduler.timetable import Timetable
from synergy.scheduler.thread_handler import construct_thread_handler, ThreadHandlerArguments


class Scheduler(SynergyProcess):
    """ Scheduler hosts multiple state machines, and logic for triggering jobs """

    def __init__(self, process_name):
        super(Scheduler, self).__init__(process_name)
        self.lock = Lock()
        self.logger.info('Starting %s' % self.process_name)
        self.publishers = PublishersPool(self.logger)
        self.managed_handlers = dict()
        self.freerun_handlers = dict()
        self.timetable = Timetable(self.logger)
        self.state_machines = self._construct_state_machines()

        self.freerun_process_dao = FreerunProcessDao(self.logger)
        self.mx = None
        self.bus_listener = None
        self.logger.info('Started %s' % self.process_name)

    def __del__(self):
        for key, handler in self.managed_handlers.items():
            handler.deactivate(update_persistent=False)
        self.managed_handlers.clear()

        for key, handler in self.freerun_handlers.items():
            handler.deactivate(update_persistent=False)
        self.freerun_handlers.clear()

        self.publishers.close()

        super(Scheduler, self).__del__()

    def _construct_state_machines(self):
        """ :return: dict in format <state_machine_common_name: instance_of_the_state_machine> """
        state_machines = dict()
        for state_machine in [StateMachineContinuous(self.logger, self.timetable),
                              StateMachineDiscrete(self.logger, self.timetable),
                              StateMachineSimpleDiscrete(self.logger, self.timetable),
                              StateMachineFreerun(self.logger)]:
            state_machines[state_machine.name] = state_machine
        return state_machines

    def _register_process_entry(self, process_entry, call_back):
        """ method parses process_entry and creates a timer_handler out of it
         timer_handler is enlisted to either :self.freerun_handlers or :self.managed_handlers
         timer_handler is started, unless it is marked as STATE_OFF """
        handler = construct_thread_handler(self.logger, process_entry, call_back)

        if handler.is_managed:
            self.managed_handlers[handler.key] = handler
        elif handler.is_freerun:
            self.freerun_handlers[handler.key] = handler
        else:
            self.logger.error('Process/Handler type %s is not known to the system. Skipping it.'
                              % handler.handler_type)
            return

        if process_entry.is_on:
            handler.activate()
            self.logger.info('Started scheduler thread for %s:%r.'
                             % (handler.handler_type, handler.key))
        else:
            self.logger.info('Handler for %s:%r registered in Scheduler. Idle until activated.'
                             % (handler.handler_type, handler.key))

    # **************** Scheduler Methods ************************
    def _load_managed_entries(self):
        """ loads scheduler managed entries. no start-up procedures are performed """
        for process_name, process_entry in context.process_context.items():
            if process_entry.process_type == TYPE_MANAGED:
                function = self.fire_managed_worker
            elif process_entry.process_type == TYPE_GARBAGE_COLLECTOR:
                function = self.fire_garbage_collector
            elif process_entry.process_type in [TYPE_FREERUN, TYPE_DAEMON]:
                self.logger.info('%s of type %s is found in context, but not managed by Synergy Scheduler. '
                                 'Skipping the process.'
                                 % (process_name, process_entry.process_type.upper()))
                continue
            else:
                self.logger.error('Type %s of process %s is not known to the system. Skipping it.' %
                                  (process_entry.process_type, process_name))
                continue

            try:
                self._register_process_entry(process_entry, function)
            except Exception:
                self.logger.error('Scheduler Handler %r failed to start. Skipping it.' % (process_entry.key,),
                                  exc_info=True)

    def _load_freerun_entries(self):
        """ reads scheduler managed entries and starts their timers to trigger events """
        freerun_entries = self.freerun_process_dao.get_all()
        for freerun_entry in freerun_entries:
            try:
                self._register_process_entry(freerun_entry, self.fire_freerun_worker)
            except Exception:
                self.logger.error('Scheduler Handler %r failed to start. Skipping it.' % (freerun_entry.key,),
                                  exc_info=True)

    @with_reconnect
    def start(self, *_):
        """ reads managed process entries and starts timer instances; starts MX thread """
        db_manager.synch_db()
        self._load_managed_entries()

        try:
            self._load_freerun_entries()
        except LookupError as e:
            self.logger.warn('DB Lookup: %s' % str(e))

        # Scheduler is initialized and running. Status Bus Listener can be safely started
        self.bus_listener = StatusBusListener(self)
        self.bus_listener.start()

        # All Scheduler components are initialized and running. Management Extension (MX) can be safely started
        self.mx = MX(self)
        self.mx.start_mx_thread()

    @thread_safe
    def fire_managed_worker(self, thread_handler_arguments):
        """requests next valid job for given process and manages its state"""

        def _fire_worker(process_entry):
            assert isinstance(process_entry, ManagedProcessEntry)
            job_record = self.timetable.get_next_job_record(process_entry.process_name)
            state_machine = self.state_machines[process_entry.state_machine_name]

            run_on_active_timeperiod = process_entry.run_on_active_timeperiod
            if not run_on_active_timeperiod:
                time_qualifier = process_entry.time_qualifier
                incremented_timeperiod = time_helper.increment_timeperiod(time_qualifier, job_record.timeperiod)
                dt_record_timestamp = time_helper.synergy_to_datetime(time_qualifier, incremented_timeperiod)
                dt_record_timestamp += timedelta(minutes=LAG_5_MINUTES)

                if datetime.utcnow() <= dt_record_timestamp:
                    self.logger.info('Job record %s for timeperiod %s will not be triggered until %s.'
                                     % (job_record.db_id,
                                        job_record.timeperiod,
                                        dt_record_timestamp.strftime('%Y-%m-%d %H:%M:%S')))
                    return None

            blocking_type = process_entry.blocking_type
            if blocking_type == BLOCKING_DEPENDENCIES:
                state_machine.manage_job_with_blocking_dependencies(job_record, run_on_active_timeperiod)
            elif blocking_type == BLOCKING_CHILDREN:
                state_machine.manage_job_with_blocking_children(job_record, run_on_active_timeperiod)
            elif blocking_type == BLOCKING_NORMAL:
                state_machine.manage_job(job_record)
            else:
                raise ValueError('Unknown managed process type %s' % blocking_type)

            return job_record

        try:
            assert isinstance(thread_handler_arguments, ThreadHandlerArguments)
            self.logger.info('%r {' % (thread_handler_arguments.key, ))

            job_record = _fire_worker(thread_handler_arguments.process_entry)
            while job_record and job_record.is_finished:
                job_record = _fire_worker(thread_handler_arguments.process_entry)

        except (AMQPError, IOError) as e:
            self.logger.error('AMQPError: %s' % str(e), exc_info=True)
            self.publishers.reset_all(suppress_logging=True)
        except Exception as e:
            self.logger.error('Exception: %s' % str(e), exc_info=True)
        finally:
            self.logger.info('}')

    @thread_safe
    def fire_freerun_worker(self, thread_handler_arguments):
        """fires free-run worker with no dependencies to track"""
        try:
            assert isinstance(thread_handler_arguments, ThreadHandlerArguments)
            self.logger.info('%r {' % (thread_handler_arguments.key, ))

            state_machine = self.state_machines[STATE_MACHINE_FREERUN]
            state_machine.manage_schedulable(thread_handler_arguments.process_entry)

        except Exception as e:
            self.logger.error('fire_freerun_worker: %s' % str(e))
        finally:
            self.logger.info('}')

    @thread_safe
    def fire_garbage_collector(self, thread_handler_arguments):
        """fires garbage collector to re-trigger invalid unit_of_work"""
        try:
            assert isinstance(thread_handler_arguments, ThreadHandlerArguments)
            self.logger.info('%r {' % (thread_handler_arguments.key, ))
            mq_request = SynergyMqTransmission(process_name=thread_handler_arguments.key)

            publisher = self.publishers.get(thread_handler_arguments.key)
            publisher.publish(mq_request.document)
            publisher.release()
            self.logger.info('Published trigger for %s' % thread_handler_arguments.key)

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
