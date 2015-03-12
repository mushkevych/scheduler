__author__ = 'Bohdan Mushkevych'

import unittest

from tests import base_fixtures
from synergy.system import time_helper
from synergy.system.time_qualifier import QUALIFIER_HOURLY, QUALIFIER_DAILY
from constants import PROCESS_SITE_HOURLY, PROCESS_SITE_DAILY, TOKEN_SITE
from synergy.scheduler.tree_node import TreeNode
from synergy.scheduler.tree import MultiLevelTree
from synergy.conf import settings


class TestTwoLevelTree(unittest.TestCase):
    def setUp(self):
        self.initial_actual_timeperiod = time_helper.actual_timeperiod
        self.initial_synergy_start_time = settings.settings['synergy_start_timeperiod']

        self.actual_timeperiod = time_helper.actual_timeperiod(QUALIFIER_HOURLY)
        settings.settings['synergy_start_timeperiod'] = self.actual_timeperiod

        self.tree = MultiLevelTree(process_names=[PROCESS_SITE_HOURLY, PROCESS_SITE_DAILY],
                                   mx_name=TOKEN_SITE, mx_page='some_mx_page')

    def tearDown(self):
        del self.tree
        settings.settings['synergy_start_timeperiod'] = self.initial_synergy_start_time
        time_helper.actual_timeperiod = self.initial_actual_timeperiod

    def test_simple_build_tree(self):
        self.tree.build_tree()
        self.assertEqual(len(self.tree.root.children), 1)

        hourly_timeperiod = time_helper.actual_timeperiod(QUALIFIER_HOURLY)
        daily_timeperiod = time_helper.actual_timeperiod(QUALIFIER_DAILY)
        self.assertIn(daily_timeperiod, self.tree.root.children)

        daily_node = self.tree.root.children[daily_timeperiod]
        self.assertIn(hourly_timeperiod, daily_node.children)
        self.assertEqual(daily_node.timeperiod, daily_timeperiod)
        self.assertEqual(daily_node.children[hourly_timeperiod].timeperiod, hourly_timeperiod)

    def _perform_assertions(self, start_timeperiod, delta):
        def calculate_leafs(tree_node):
            assert isinstance(tree_node, TreeNode)
            if not tree_node.children:
                # node is the leaf
                return 1

            counter = 0
            for child_timeperiod, child_node in tree_node.children.items():
                counter += calculate_leafs(child_node)
            return counter

        number_of_leafs = calculate_leafs(self.tree.root)
        self.assertEqual(number_of_leafs, delta + 1, 'Expected number of leaf nodes was %d, while actual is %d'
                         % (delta + 1, number_of_leafs))

    def test_less_simple_build_tree(self):
        delta = 105
        new_synergy_start_time = base_fixtures.wind_the_time(QUALIFIER_HOURLY,
                                                             self.actual_timeperiod,
                                                             -delta)
        settings.settings['synergy_start_timeperiod'] = new_synergy_start_time

        self.tree.build_tree()
        self._perform_assertions(new_synergy_start_time, delta)

    def test_catching_up_time_build_tree(self):
        delta = 5
        new_synergy_start_time = base_fixtures.wind_the_time(QUALIFIER_HOURLY,
                                                             self.actual_timeperiod,
                                                             -delta)
        settings.settings['synergy_start_timeperiod'] = new_synergy_start_time

        self.tree.build_tree()
        self._perform_assertions(new_synergy_start_time, delta)

        new_actual_timeperiod = base_fixtures.wind_the_time(QUALIFIER_HOURLY,
                                                            self.actual_timeperiod,
                                                            delta)

        time_helper.actual_timeperiod = \
            base_fixtures.wind_actual_timeperiod(time_helper.synergy_to_datetime(QUALIFIER_HOURLY,
                                                                                 new_actual_timeperiod))
        self.tree.build_tree()
        self._perform_assertions(new_synergy_start_time, 2 * delta)

    def test_is_managing_process(self):
        self.assertIn(PROCESS_SITE_HOURLY, self.tree)
        self.assertNotIn('AnyOtherProcess', self.tree)


if __name__ == '__main__':
    unittest.main()
