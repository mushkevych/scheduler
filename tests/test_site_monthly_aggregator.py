__author__ = 'Bohdan Mushkevych'

import unittest
from settings import enable_test_mode
enable_test_mode()

from tests import daily_fixtures
from tests import monthly_fixtures
from tests.test_abstract_worker import AbstractWorkerUnitTest
from workers.site_monthly_aggregator import SiteMonthlyAggregator
from synergy.db.model import base_model
from constants import PROCESS_SITE_MONTHLY
from model.raw_data import DOMAIN_NAME


class SiteMonthlyAggregatorUnitTest(AbstractWorkerUnitTest):
    def virtual_set_up(self):
        super(SiteMonthlyAggregatorUnitTest, self).constructor(baseclass=SiteMonthlyAggregator,
                                                               process_name=PROCESS_SITE_MONTHLY,
                                                               output_prefix='EXPECTED_MONTHLY_SITE',
                                                               output_module=monthly_fixtures,
                                                               generate_output=True,
                                                               compare_results=False)

        daily_fixtures.clean_site_entries()
        return daily_fixtures.generated_site_entries()

    def virtual_tear_down(self):
        daily_fixtures.clean_site_entries()

    def _get_key(self, obj):
        return obj[DOMAIN_NAME], obj[base_model.TIMEPERIOD]

    def test_aggregation(self):
        super(SiteMonthlyAggregatorUnitTest, self).perform_aggregation()


if __name__ == '__main__':
    unittest.main()
