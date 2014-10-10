__author__ = 'Bohdan Mushkevych'

import unittest
from tests import daily_fixtures
from tests.test_abstract_worker import AbstractWorkerUnitTest
from synergy.db.model import base_model
from workers.site_hourly_aggregator import SiteHourlyAggregator
from constants import PROCESS_SITE_DAILY
from model.raw_data import DOMAIN_NAME


class HourlySiteAggregatorUnitTest(AbstractWorkerUnitTest):
    def virtual_set_up(self):
        super(HourlySiteAggregatorUnitTest, self).constructor(SiteHourlyAggregator,
                                                              PROCESS_SITE_DAILY,
                                                              'EXPECTED_DAILY_SITE',
                                                              daily_fixtures,
                                                              False,
                                                              True)
        daily_fixtures.clean_site_hourly_entries()
        return daily_fixtures.generated_site_hourly_entries()

    def virtual_tear_down(self):
        daily_fixtures.clean_site_hourly_entries()

    def _get_key(self, obj):
        return obj[DOMAIN_NAME], obj[base_model.TIMEPERIOD]

    def test_aggregation(self):
        super(HourlySiteAggregatorUnitTest, self).perform_aggregation()


if __name__ == '__main__':
    unittest.main()
