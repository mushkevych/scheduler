__author__ = 'Bohdan Mushkevych'

import unittest
from mq.flopsy import PublishersPool
from db.dao.unit_of_work_dao import UnitOfWorkDao
from tests.base_fixtures import create_and_insert_unit_of_work
from system.process_context import PROCESS_SITE_DAILY
from tests.base_fixtures import TestMessage
from workers.site_daily_aggregator import SiteDailyAggregator


class DailySiteAggregatorUnitTest(unittest.TestCase):
    def setUp(self):
        process_name = PROCESS_SITE_DAILY
        self.aggregator = SiteDailyAggregator(process_name)
        self.publishers = PublishersPool(self.aggregator.logger)
        self.uow_id = create_and_insert_unit_of_work(PROCESS_SITE_DAILY,
                                                     0,
                                                     1,
                                                     2011091220)

    def tearDown(self):
        # cleaning up DB
        uow_dao = UnitOfWorkDao(self.aggregator.logger)
        uow_dao.remove(self.uow_id)
        del self.aggregator

    def test_aggregation(self):
        message = TestMessage()
        message.body = str(self.uow_id)
        self.aggregator._mq_callback(message)


if __name__ == '__main__':
    unittest.main()
