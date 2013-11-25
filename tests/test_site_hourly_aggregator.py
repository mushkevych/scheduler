__author__ = 'Bohdan Mushkevych'

import unittest
from db.model import base_model
from tests import hourly_fixtures
from system.process_context import PROCESS_SITE_HOURLY
from tests.test_abstract_worker import AbstractWorkerUnitTest
from workers.site_hourly_aggregator import SiteHourlyAggregator


class HourlySiteAggregatorUnitTest(AbstractWorkerUnitTest):
    def virtual_set_up(self):
        super(HourlySiteAggregatorUnitTest, self).constructor(SiteHourlyAggregator,
                                                              PROCESS_SITE_HOURLY,
                                                              'EXPECTED_HOURLY_SITE',
                                                              hourly_fixtures,
                                                              False,
                                                              True)
        hourly_fixtures.clean_session_entries()
        return hourly_fixtures.generated_session_entries()

    def virtual_tear_down(self):
        hourly_fixtures.clean_session_entries()

    def _get_key(self, obj):
        return obj[base_model.KEY], obj[base_model.TIMEPERIOD]

    def test_aggregation(self):
        super(HourlySiteAggregatorUnitTest, self).perform_aggregation()


if __name__ == '__main__':
    unittest.main()
