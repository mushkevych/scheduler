__author__ = 'Bohdan Mushkevych'

from settings import settings
from flopsy.flopsy import Consumer
from system.performance_ticker import WorkerPerformanceTicker
from system.synergy_process import SynergyProcess

from amqplib.client_0_8 import AMQPException
from threading import Thread


class AbstractWorker(SynergyProcess):
    """
    class works as an abstract basement for all workers and aggregators
    it registers in the mq and awaits for the messages
    """

    def __init__(self, process_name):
        """@param process_name: id of the process, the worker will be performing """
        super(AbstractWorker, self).__init__(process_name)
        self._init_performance_ticker(self.logger)

        msg_suffix = 'in Production Mode'
        if settings['under_test']:
            msg_suffix = 'in Testing Mode'
        self.logger.info('Started %s %s' % (self.process_name, msg_suffix))

    def __del__(self):
        try:
            self.performance_ticker.cancel()
        except Exception as e:
            self.logger.error('Exception caught while cancelling the performance_ticker: %s' % str(e))
        super(AbstractWorker, self).__del__()

    # ********************** abstract methods ****************************
    def _init_performance_ticker(self, logger):
        self.performance_ticker = WorkerPerformanceTicker(logger)
        self.performance_ticker.start()

    # ********************** thread-related methods ****************************
    def _mq_callback(self, message):
        """ abstract method to process messages from MQ 
        @param message: mq message"""
        pass

    def _run_mq_listener(self):
        try:
            self.consumer = Consumer(self.process_name)
            self.consumer.register(self._mq_callback)
            self.consumer.wait()
        except (AMQPException, IOError) as e:
            self.logger.error('AMQPException: %s' % str(e))
        finally:
            self.__del__()
            self.logger.info('Exiting main thread. All auxiliary threads stopped.')

    def start(self, *args):
        self.main_thread = Thread(target=self._run_mq_listener)
        self.main_thread.start()
