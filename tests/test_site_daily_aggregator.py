
__author__ = 'Bohdan Mushkevych'

import unittest

from settings import enable_test_mode

enable_test_mode()


from tests import hourly_fixtures, daily_fixtures
from tests.test_abstract_worker import AbstractWorkerUnitTest
from synergy.db.model import base_model
from workers.site_daily_aggregator import SiteDailyAggregator
from constants import PROCESS_SITE_DAILY
from model.raw_data import DOMAIN_NAME


class SiteDailyAggregatorUnitTest(AbstractWorkerUnitTest):
    def virtual_set_up(self):
        super(SiteDailyAggregatorUnitTest, self).constructor(baseclass=SiteDailyAggregator,
                                                             process_name=PROCESS_SITE_DAILY,
                                                             output_prefix='EXPECTED_DAILY_SITE',
                                                             output_module=daily_fixtures,
                                                             generate_output=False,
                                                             compare_results=True)

        hourly_fixtures.clean_site_entries()
        return hourly_fixtures.generated_site_entries()

    def virtual_tear_down(self):
        hourly_fixtures.clean_site_entries()

    def _get_key(self, obj):
        return obj[DOMAIN_NAME], obj[base_model.TIMEPERIOD]

    def test_aggregation(self):
        super(SiteDailyAggregatorUnitTest, self).perform_aggregation()


if __name__ == '__main__':
    unittest.main()
