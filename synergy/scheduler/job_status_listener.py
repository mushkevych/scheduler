__author__ = 'Bohdan Mushkevych'

from threading import Thread

from amqp import AMQPError

from synergy.db.model.mq_transmission import MqTransmission
from synergy.db.dao.job_dao import JobDao
from synergy.scheduler.scheduler_constants import QUEUE_JOB_STATUS
from synergy.scheduler.thread_handler import ManagedThreadHandler
from synergy.mq.flopsy import Consumer


class JobStatusListener(object):
    """ class instance listens to the QUEUE_JOB_STATUS queue and triggers ManagedThreadHandlers if applicable """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.timetable = scheduler.timetable
        self.logger = scheduler.logger
        self.job_dao = JobDao(self.logger)
        self.consumer = Consumer(QUEUE_JOB_STATUS)
        self.main_thread = None

    def __del__(self):
        try:
            self.logger.info('JobStatusListener: Closing Flopsy Consumer...')
            self.consumer.close()
        except Exception as e:
            self.logger.error(f'JobStatusListener: Exception caught while closing Flopsy Consumer: {e}')

    # ********************** thread-related methods ****************************
    def _mq_callback(self, message):
        """ method receives a message from Synergy Scheduler notifying of Job completion,
            builds up a list of dependant TreeNodes/Jobs and triggers their ManagedThreadHandlers, if applicable
            :param message: <MqTransmission> mq message """
        try:
            self.logger.info('JobStatusListener {')

            mq_request = MqTransmission.from_json(message.body)
            job_record = self.job_dao.get_by_id(mq_request.process_name, mq_request.record_db_id)

            # step 1: identify dependant tree nodes
            tree_obj = self.timetable.get_tree(job_record.process_name)
            tree_node = tree_obj.get_node(job_record.process_name, job_record.timeperiod)
            dependant_nodes = self.timetable._find_dependant_tree_nodes(tree_node)

            # step 2: form list of handlers to trigger
            handlers_to_trigger = set()
            for node in dependant_nodes:
                state_machine = self.scheduler.state_machine_for(node.process_name)
                if state_machine.run_on_active_timeperiod:
                    # ignore dependant processes whose state machine can run on an active timeperiod
                    # to avoid "over-triggering" them
                    continue
                handlers_to_trigger.add(self.scheduler.managed_handlers[node.process_name])

            # step 3: iterate the list of handlers and trigger them
            for handler in handlers_to_trigger:
                assert isinstance(handler, ManagedThreadHandler)
                handler.trigger()

        except KeyError:
            self.logger.error(f'Access error for {message.body}', exc_info=True)
        except Exception:
            self.logger.error(f'Error during ManagedThreadHandler.trigger call {message.body}', exc_info=True)
        finally:
            self.consumer.acknowledge(message.delivery_tag)
            self.logger.info('JobStatusListener }')

    def _run_mq_listener(self):
        try:
            self.consumer.register(self._mq_callback)
            self.logger.info('JobStatusListener: instantiated and activated.')
            self.consumer.wait()
        except (AMQPError, IOError) as e:
            self.logger.error(f'JobStatusListener: AMQPError {e}')
        finally:
            self.__del__()
            self.logger.info('JobStatusListener: Shut down.')

    def start(self, *_):
        self.main_thread = Thread(target=self._run_mq_listener)
        self.main_thread.daemon = True
        self.main_thread.start()

    def stop(self):
        """ method stops currently MQ Consumer listener, if any """
        self.__del__()
