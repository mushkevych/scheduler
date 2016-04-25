__author__ = 'Bohdan Mushkevych'

import unittest
try:
    import mock
except ImportError:
    from unittest import mock

from settings import enable_test_mode
enable_test_mode()

from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.dao.uow_log_dao import UowLogDao
from synergy.system.system_logger import get_logger
from synergy.system.mq_transmitter import MqTransmitter
from synergy.workers.abstract_uow_aware_worker import AbstractUowAwareWorker
from tests.base_fixtures import create_and_insert_unit_of_work, TestMessage
from context import PROCESS_ALERT_DAILY

INFO_LOG_MESSAGES = ['111222333 INFO log message string {0}'.format(x) for x in range(10)]
WARN_LOG_MESSAGES = ['444555666 WARNING log message string {0}'.format(x) for x in range(10)]
STD_MESSAGES = ['777888999 STD OUTPUT message {0}'.format(x) for x in range(10)]


class TheWorker(AbstractUowAwareWorker):
    def __init__(self, process_name):
        super(TheWorker, self).__init__(process_name, perform_db_logging=True)
        self.mq_transmitter = mock.create_autospec(MqTransmitter)

    def _init_performance_tracker(self, logger):
        super(TheWorker, self)._init_performance_tracker(logger)
        self.performance_tracker.cancel()

    def _init_mq_consumer(self):
        self.consumer = mock.Mock()


class ChattyWorker(TheWorker):
    def _process_uow(self, uow):
        for message in INFO_LOG_MESSAGES:
            self.logger.info(message)
        for message in WARN_LOG_MESSAGES:
            self.logger.warn(message)
        for message in STD_MESSAGES:
            print(message)


class ExceptionWorker(TheWorker):
    def _process_uow(self, uow):
        try:
            raise ValueError('Artificially triggered exception to test Uow Exception Logging')
        except Exception as e:
            self.logger.error('Exception: {0}'.format(e), exc_info=True)


class UowLogHandlerUnitTest(unittest.TestCase):
    """ Test flow:
        1. create a UOW in the database
        2. emulate mq message
        3. call _mq_callback method
        4. validate that all the messages are now found in the uow_log record
        5. remove UOW and uow_log record
    """

    def setUp(self):
        self.process_name = PROCESS_ALERT_DAILY
        self.logger = get_logger(self.process_name)
        self.uow_id = create_and_insert_unit_of_work(self.process_name, 'range_start', 'range_end')
        self.uow_id = str(self.uow_id)
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.uow_log_dao = UowLogDao(self.logger)

    def tearDown(self):
        self.uow_dao.remove(self.uow_id)
        self.uow_log_dao.remove(self.uow_id)

    def test_logging(self):
        self.worker = ChattyWorker(self.process_name)
        message = TestMessage(process_name=self.process_name, uow_id=self.uow_id)
        self.worker._mq_callback(message)

        uow_log = self.uow_log_dao.get_one(self.uow_id)
        messages = INFO_LOG_MESSAGES + WARN_LOG_MESSAGES  # + STD_MESSAGES
        self.assertLessEqual(len(messages), len(uow_log.log))

        for index, message in enumerate(messages):
            self.assertIn(message, uow_log.log[index])

    def test_exception_logging(self):
        self.worker = ExceptionWorker(self.process_name)
        message = TestMessage(process_name=self.process_name, uow_id=self.uow_id)
        self.worker._mq_callback(message)

        uow_log = self.uow_log_dao.get_one(self.uow_id)
        messages = ['Exception: Artificially triggered exception to test Uow Exception Logging',
                    'method ExceptionWorker._process_uow returned None. Assuming happy flow.',
                    'at INVALID_TIMEPERIOD: Success/Failure 0/0']

        for index, message in enumerate(messages):
            self.assertIn(message, uow_log.log[index])


if __name__ == '__main__':
    unittest.main()
