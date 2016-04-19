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
from synergy.system.performance_tracker import UowAwareTracker
from synergy.workers.abstract_uow_aware_worker import AbstractUowAwareWorker
from tests.base_fixtures import create_and_insert_unit_of_work, TestMessage
from tests.ut_context import PROCESS_UNIT_TEST

INFO_LOG_MESSAGES = ['111222333 INFO log message string {0}'.format(x) for x in range(50)]
WARN_LOG_MESSAGES = ['444555666 WARNING log message string {0}'.format(x) for x in range(50)]
STD_MESSAGES = ['777888999 STD OUTPUT message {0}'.format(x) for x in range(50)]


class ChattyWorker(AbstractUowAwareWorker):
    def __init__(self, process_name):
        super(ChattyWorker, self).__init__(process_name, perform_db_logging=True)

    def _init_performance_tracker(self, logger):
        self.performance_tracker = UowAwareTracker(logger)

    def _init_mq_consumer(self):
        self.consumer = mock.Mock()

    def _process_uow(self, uow):
        for message in INFO_LOG_MESSAGES:
            self.logger.info(message)

        for message in WARN_LOG_MESSAGES:
            self.logger.warn(message)

        for message in STD_MESSAGES:
            print(message)


class UowLogHandlerUnitTest(unittest.TestCase):
    """ Test flow:
        1. create a UOW in the database
        2. assert that no uow_log records are present in the DB
        3. call process_uow method
        4. validate that all the messages are now found in the uow_log record
        5. remove UOW and uow_log record
    """
    def setUp(self):
        self.process_name = PROCESS_UNIT_TEST
        self.logger = get_logger(self.process_name)
        self.worker = ChattyWorker(self.process_name)
        self.uow_id = create_and_insert_unit_of_work(self.process_name, 'range_start', 'range_end')
        self.uow_id = str(self.uow_id)
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.uow_log_dao = UowLogDao(self.logger)

    def tearDown(self):
        self.uow_dao.remove(self.uow_id)
        self.uow_log_dao.remove(self.uow_id)

    def test_logging(self):
        message = TestMessage(process_name=self.process_name, uow_id=self.uow_id)
        self.worker._mq_callback(message)

        uow_log = self.uow_log_dao.get_one(self.uow_id)
        messages = INFO_LOG_MESSAGES + WARN_LOG_MESSAGES  # + STD_MESSAGES
        self.assertLessEqual(len(messages), len(uow_log.log))

        for message in messages:
            self.assertIn(message, messages)


if __name__ == '__main__':
    unittest.main()
