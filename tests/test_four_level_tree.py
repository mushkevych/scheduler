__author__ = 'Bohdan Mushkevych'

import unittest
from tests.base_fixtures import wind_the_time, wind_actual_timeperiod
from constants import PROCESS_SITE_HOURLY, TOKEN_SITE, PROCESS_SITE_YEARLY, PROCESS_SITE_MONTHLY, PROCESS_SITE_DAILY
from tests.ut_context import PROCESS_UNIT_TEST
from system import time_helper
from system.time_qualifier import *
from scheduler.tree import FourLevelTree
from conf import settings


class TestFourLevelTree(unittest.TestCase):
    def setUp(self):
        self.initial_actual_timeperiod = time_helper.actual_timeperiod
        self.initial_synergy_start_time = settings.settings['synergy_start_timeperiod']
        self.tree = FourLevelTree(PROCESS_SITE_YEARLY,
                                  PROCESS_SITE_MONTHLY,
                                  PROCESS_SITE_DAILY,
                                  PROCESS_SITE_HOURLY,
                                  TOKEN_SITE,
                                  'some_mx_page')

    def tearDown(self):
        del self.tree
        settings.settings['synergy_start_timeperiod'] = self.initial_synergy_start_time
        time_helper.actual_timeperiod = self.initial_actual_timeperiod

    def test_simple_build_tree(self):
        self.tree.build_tree()

        actual_yearly_timeperiod = time_helper.actual_timeperiod(QUALIFIER_YEARLY)
        actual_monthly_timeperiod = time_helper.actual_timeperiod(QUALIFIER_MONTHLY)
        actual_daily_timeperiod = time_helper.actual_timeperiod(QUALIFIER_DAILY)
        actual_hourly_timeperiod = time_helper.actual_timeperiod(QUALIFIER_HOURLY)
        assert len(self.tree.root.children) == 1
        assert actual_yearly_timeperiod in self.tree.root.children
        self.assertEqual(self.tree.get_node_by_process(PROCESS_SITE_YEARLY, actual_yearly_timeperiod).process_name,
                         PROCESS_SITE_YEARLY)
        self.assertEqual(self.tree.get_node_by_process(PROCESS_SITE_YEARLY, actual_yearly_timeperiod).timeperiod,
                         actual_yearly_timeperiod)
        self.assertEqual(self.tree.get_node_by_process(PROCESS_SITE_YEARLY, actual_yearly_timeperiod).time_qualifier,
                         QUALIFIER_YEARLY)

        assert len(self.tree.root.children[actual_yearly_timeperiod].children) == 1
        assert actual_monthly_timeperiod in self.tree.root.children[actual_yearly_timeperiod].children
        self.assertEqual(self.tree.get_node_by_process(PROCESS_SITE_MONTHLY, actual_monthly_timeperiod).process_name,
                         PROCESS_SITE_MONTHLY)
        self.assertEqual(self.tree.get_node_by_process(PROCESS_SITE_MONTHLY, actual_monthly_timeperiod).timeperiod,
                         actual_monthly_timeperiod)
        self.assertEqual(self.tree.get_node_by_process(PROCESS_SITE_MONTHLY, actual_monthly_timeperiod).time_qualifier,
                         QUALIFIER_MONTHLY)

        assert len(self.tree.root.children[actual_yearly_timeperiod].children[actual_monthly_timeperiod].children) == 1
        assert actual_daily_timeperiod in self.tree.root.children[actual_yearly_timeperiod].children[
            actual_monthly_timeperiod].children
        self.assertEqual(self.tree.get_node_by_process(PROCESS_SITE_DAILY, actual_daily_timeperiod).process_name,
                         PROCESS_SITE_DAILY)
        self.assertEqual(self.tree.get_node_by_process(PROCESS_SITE_DAILY, actual_daily_timeperiod).timeperiod,
                         actual_daily_timeperiod)
        self.assertEqual(self.tree.get_node_by_process(PROCESS_SITE_DAILY, actual_daily_timeperiod).time_qualifier,
                         QUALIFIER_DAILY)

        assert len(self.tree.root.children[actual_yearly_timeperiod].children[actual_monthly_timeperiod].
                   children[actual_daily_timeperiod].children) == 1
        assert actual_hourly_timeperiod in self.tree.root.children[actual_yearly_timeperiod].children[
            actual_monthly_timeperiod].children[actual_daily_timeperiod].children
        self.assertEqual(self.tree.get_node_by_process(PROCESS_SITE_HOURLY, actual_hourly_timeperiod).process_name,
                         PROCESS_SITE_HOURLY)
        self.assertEqual(self.tree.get_node_by_process(PROCESS_SITE_HOURLY, actual_hourly_timeperiod).timeperiod,
                         actual_hourly_timeperiod)
        self.assertEqual(self.tree.get_node_by_process(PROCESS_SITE_HOURLY, actual_hourly_timeperiod).time_qualifier,
                         QUALIFIER_HOURLY)

    def _perform_assertions(self, start_timeperiod, delta):
        yearly_timeperiod = time_helper.cast_to_time_qualifier(QUALIFIER_YEARLY,
                                                               start_timeperiod)
        monthly_timeperiod = time_helper.cast_to_time_qualifier(QUALIFIER_MONTHLY,
                                                                start_timeperiod)
        daily_timeperiod = time_helper.cast_to_time_qualifier(QUALIFIER_DAILY,
                                                              start_timeperiod)
        hourly_timeperiod = time_helper.cast_to_time_qualifier(QUALIFIER_HOURLY,
                                                               start_timeperiod)

        number_of_leafs = 0
        for yt, yearly_root in sorted(self.tree.root.children.items(), key=lambda x: x[0]):
            self.assertEqual(yearly_timeperiod, yt)

            for mt, monthly_root in sorted(yearly_root.children.items(), key=lambda x: x[0]):
                self.assertEqual(monthly_timeperiod, mt)

                for dt, daily_root in sorted(monthly_root.children.items(), key=lambda x: x[0]):
                    self.assertEqual(daily_timeperiod, dt)

                    for ht, hourly_root in sorted(daily_root.children.items(), key=lambda x: x[0]):
                        self.assertEqual(hourly_timeperiod, ht)
                        number_of_leafs += 1
                        hourly_timeperiod = time_helper.increment_timeperiod(QUALIFIER_HOURLY, hourly_timeperiod)

                    daily_timeperiod = time_helper.increment_timeperiod(QUALIFIER_DAILY, daily_timeperiod)

                monthly_timeperiod = time_helper.increment_timeperiod(QUALIFIER_MONTHLY, monthly_timeperiod)

            yearly_timeperiod = time_helper.increment_timeperiod(QUALIFIER_YEARLY, yearly_timeperiod)

        self.assertEqual(number_of_leafs, delta + 1, 'Expected number of daily nodes was %d, while actual is %d'
                                                     % (delta + 1, number_of_leafs))

    def test_less_simple_build_tree(self):
        delta = 5 * 24  # 5 days
        new_synergy_start_time = wind_the_time(QUALIFIER_HOURLY, self.initial_synergy_start_time, -delta)

        settings.settings['synergy_start_timeperiod'] = new_synergy_start_time
        self.tree.build_tree()
        self._perform_assertions(new_synergy_start_time, delta)

    def test_catching_up_time_build_tree(self):
        delta = 5 * 24
        new_synergy_start_time = wind_the_time(QUALIFIER_HOURLY, self.initial_synergy_start_time, -delta)
        settings.settings['synergy_start_timeperiod'] = new_synergy_start_time

        self.tree.build_tree()
        self._perform_assertions(new_synergy_start_time, delta)

        new_actual_timeperiod = wind_the_time(QUALIFIER_HOURLY, self.initial_synergy_start_time, delta)

        time_helper.actual_timeperiod = wind_actual_timeperiod(time_helper.synergy_to_datetime(QUALIFIER_HOURLY,
                                                                                               new_actual_timeperiod))
        self.tree.build_tree()
        self._perform_assertions(new_synergy_start_time, 2 * delta)

    def test_is_managing_process(self):
        self.assertTrue(self.tree.is_managing_process(PROCESS_SITE_YEARLY))
        self.assertTrue(self.tree.is_managing_process(PROCESS_SITE_MONTHLY))
        self.assertTrue(self.tree.is_managing_process(PROCESS_SITE_DAILY))
        self.assertTrue(self.tree.is_managing_process(PROCESS_SITE_HOURLY))
        self.assertFalse(self.tree.is_managing_process(PROCESS_UNIT_TEST))


if __name__ == '__main__':
    unittest.main()
