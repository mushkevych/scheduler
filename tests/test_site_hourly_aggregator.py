__author__ = 'Bohdan Mushkevych'

import unittest
from settings import enable_test_mode
enable_test_mode()

from db.model.raw_data import DOMAIN_NAME
from constants import PROCESS_SITE_HOURLY
from tests import hourly_fixtures
from tests.test_abstract_worker import AbstractWorkerUnitTest
from synergy.db.model import base_model
from workers.site_hourly_aggregator import SiteHourlyAggregator


class SiteHourlyAggregatorUnitTest(AbstractWorkerUnitTest):
    def virtual_set_up(self):
        super(SiteHourlyAggregatorUnitTest, self).constructor(baseclass=SiteHourlyAggregator,
                                                              process_name=PROCESS_SITE_HOURLY,
                                                              output_prefix='EXPECTED_SITE_HOURLY',
                                                              output_module=hourly_fixtures,
                                                              generate_output=False,
                                                              compare_results=True)
        hourly_fixtures.clean_session_entries()
        return hourly_fixtures.generated_session_entries()

    def virtual_tear_down(self):
        hourly_fixtures.clean_session_entries()

    def _get_key(self, obj):
        return obj[DOMAIN_NAME], obj[base_model.TIMEPERIOD]

    def test_aggregation(self):
        super(SiteHourlyAggregatorUnitTest, self).perform_aggregation()


if __name__ == '__main__':
    unittest.main()
