"""
Created on 2011-02-21

@author: Bohdan Mushkevych
"""

import unittest
from model.abstract_model import AbstractModel
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
        return obj[AbstractModel.DOMAIN_NAME], obj[AbstractModel.TIMESTAMP]

    def test_aggregation(self):
        super(HourlySiteAggregatorUnitTest, self).perform_aggregation()


if __name__ == '__main__':
    unittest.main()
