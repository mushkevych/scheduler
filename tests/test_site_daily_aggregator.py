__author__ = 'Bohdan Mushkevych'

import unittest
from flopsy.flopsy import PublishersPool
from model import unit_of_work_dao
from model import unit_of_work
from model.unit_of_work import UnitOfWork
from system.process_context import PROCESS_SITE_DAILY
from tests.base_fixtures import TestMessage
from workers.site_daily_aggregator import SiteDailyAggregator


class DailySiteAggregatorUnitTest(unittest.TestCase):
    def create_unit_of_work(self, logger, process_name, first_object_id, last_object_id, timeperiod):
        """ method is used to insert unit_of_work """
        uow = UnitOfWork()
        uow.timeperiod = timeperiod
        uow.start_timeperiod = timeperiod
        uow.end_timeperiod = timeperiod
        uow.start_id = first_object_id
        uow.end_id = last_object_id
        uow.source_collection = None
        uow.target_collection = None
        uow.state = unit_of_work.STATE_REQUESTED
        uow.process_name = process_name
        uow.number_of_retries = 0

        uow_id = unit_of_work_dao.insert(logger, uow)
        return uow_id

    def setUp(self):
        process_name = PROCESS_SITE_DAILY
        self.aggregator = SiteDailyAggregator(process_name)
        self.publishers = PublishersPool(self.aggregator.logger)
        self.uow_id = self.create_unit_of_work(self.aggregator.logger,
                                               process_name,
                                               0,
                                               1,
                                               2011091220)

    def tearDown(self):
        # cleaning up DB
        unit_of_work_dao.remove(self.aggregator.logger, self.uow_id)
        del self.aggregator

    def test_aggregation(self):
        message = TestMessage()
        message.body = str(self.uow_id)
        self.aggregator._mq_callback(message)


if __name__ == '__main__':
    unittest.main()
