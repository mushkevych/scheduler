from synergy.db.model import base_model

__author__ = 'Bohdan Mushkevych'

import unittest
from tests import hourly_fixtures
from tests.test_abstract_worker import AbstractWorkerUnitTest
from synergy.workers.site_hourly_aggregator import SiteHourlyAggregator
from constants import PROCESS_SITE_HOURLY


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
