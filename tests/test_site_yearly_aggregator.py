__author__ = 'Bohdan Mushkevych'

import unittest
from settings import enable_test_mode
enable_test_mode()

from db.model.site_statistics import DOMAIN_NAME, TIMEPERIOD
from constants import PROCESS_SITE_YEARLY
from tests import monthly_fixtures
from tests import yearly_fixtures
from tests.test_abstract_worker import AbstractWorkerUnitTest
from workers.site_yearly_aggregator import SiteYearlyAggregator


class SiteYearlyAggregatorUnitTest(AbstractWorkerUnitTest):
    def virtual_set_up(self):
        super(SiteYearlyAggregatorUnitTest, self).constructor(baseclass=SiteYearlyAggregator,
                                                              process_name=PROCESS_SITE_YEARLY,
                                                              output_prefix='EXPECTED_SITE_YEARLY',
                                                              output_module=yearly_fixtures,
                                                              generate_output=False,
                                                              compare_results=True)

        monthly_fixtures.clean_site_entries()
        return monthly_fixtures.generated_site_entries()

    def virtual_tear_down(self):
        monthly_fixtures.clean_site_entries()

    def _get_key(self, obj):
        return obj[DOMAIN_NAME], obj[TIMEPERIOD]

    def test_aggregation(self):
        super(SiteYearlyAggregatorUnitTest, self).perform_aggregation()


if __name__ == '__main__':
    unittest.main()
