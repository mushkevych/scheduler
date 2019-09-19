__author__ = 'Bohdan Mushkevych'

from threading import Thread
import socket

from amqp import AMQPError

from synergy.conf import settings
from synergy.mq.flopsy import Consumer
from synergy.system.performance_tracker import SimpleTracker
from synergy.system.synergy_process import SynergyProcess


class AbstractMqWorker(SynergyProcess):
    """
    class works as an abstract basement for all workers and aggregators
    it registers in the mq and awaits for the messages
    """

    def __init__(self, process_name):
        """:param process_name: id of the process, the worker will be performing """
        super(AbstractMqWorker, self).__init__(process_name)
        self.mq_timeout_seconds = 0
        self._init_mq_timeout_seconds()

        self.consumer = None
        self._init_mq_consumer()

        self.main_thread = None
        self.performance_tracker = None
        self._init_performance_tracker(self.logger)

        msg_suffix = 'in Production Mode'
        if settings.settings['under_test']:
            msg_suffix = 'in Testing Mode'
        self.logger.info(f'Started {self.process_name} {msg_suffix}')

    def __del__(self):
        try:
            self.logger.info('Closing Flopsy Consumer...')
            self.consumer.close()
        except Exception as e:
            self.logger.error(f'Exception caught while closing Flopsy Consumer: {e}')

        try:
            self.logger.info('Canceling Performance Tracker...')
            self.performance_tracker.cancel()
        except Exception as e:
            self.logger.error(f'Exception caught while cancelling the Performance Tracker: {e}')
        super(AbstractMqWorker, self).__del__()

    # ********************** abstract methods ****************************
    def _init_performance_tracker(self, logger):
        self.performance_tracker = SimpleTracker(logger)
        self.performance_tracker.start()

    def _init_mq_consumer(self):
        self.consumer = Consumer(self.process_name)

    def _init_mq_timeout_seconds(self):
        if 'mq_timeout_sec' in settings.settings:
            self.mq_timeout_seconds = settings.settings['mq_timeout_sec']

    # ********************** thread-related methods ****************************
    def _mq_callback(self, message):
        """ abstract method to process messages from MQ
        :param message: mq message"""
        pass

    def _run_mq_listener(self):
        try:
            self.consumer.register(self._mq_callback)
            self.consumer.wait(self.mq_timeout_seconds)
        except socket.timeout as e:
            self.logger.warning(f'Queue {self.consumer.queue} is likely empty. Worker exits due to: {e}')
        except (AMQPError, IOError) as e:
            self.logger.error(f'AMQPError: {e}')
        finally:
            self.__del__()
            self.logger.info('Exiting main thread. All auxiliary threads stopped.')

    def start(self, *_):
        self.main_thread = Thread(target=self._run_mq_listener)
        self.main_thread.start()
