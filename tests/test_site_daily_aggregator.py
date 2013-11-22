"""
Created on 2011-02-25

@author: Bohdan Mushkevych
"""

import unittest
from flopsy.flopsy import PublishersPool
from model import unit_of_work_helper
from model.unit_of_work import UnitOfWork
from system.process_context import PROCESS_SITE_DAILY
from tests.base_fixtures import TestMessage
from workers.site_daily_aggregator import SiteDailyAggregator


class DailySiteAggregatorUnitTest(unittest.TestCase):

    def create_unit_of_work(self, logger, process_name, first_object_id, last_object_id, timestamp):
        """ method is used to insert unit_of_work """
        unit_of_work = UnitOfWork()
        unit_of_work.set_timestamp(timestamp)
        unit_of_work.set_start_timeperiod(timestamp)
        unit_of_work.set_end_timeperiod(timestamp)
        unit_of_work.set_start_id(first_object_id)
        unit_of_work.set_end_id(last_object_id)
        unit_of_work.set_source_collection(None)
        unit_of_work.set_target_collection(None)
        unit_of_work.set_state(UnitOfWork.STATE_REQUESTED)
        unit_of_work.set_process_name(process_name)
        unit_of_work.set_number_of_retries(0)

        uow_id = unit_of_work_helper.insert(logger, unit_of_work)
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
        unit_of_work_helper.remove(self.aggregator.logger, self.uow_id)
        del self.aggregator

    def test_aggregation(self):
        message = TestMessage()
        message.body = str(self.uow_id)
        self.aggregator._mq_callback(message)


if __name__ == '__main__':
    unittest.main()
