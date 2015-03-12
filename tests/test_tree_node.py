__author__ = 'Bohdan Mushkevych'

import unittest

from settings import enable_test_mode
enable_test_mode()

from constants import TOKEN_SITE, TOKEN_CLIENT, PROCESS_SITE_YEARLY, PROCESS_SITE_MONTHLY, \
    PROCESS_SITE_DAILY, PROCESS_SITE_HOURLY, PROCESS_CLIENT_MONTHLY
from synergy.system import time_helper
from synergy.scheduler.tree import MultiLevelTree
from synergy.system.time_qualifier import *


class TestTreeNode(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestTreeNode, cls).setUpClass()

    def setUp(self):
        self.tree_four_level = MultiLevelTree(process_names=[PROCESS_SITE_YEARLY,
                                                             PROCESS_SITE_MONTHLY,
                                                             PROCESS_SITE_DAILY,
                                                             PROCESS_SITE_HOURLY],
                                              full_name=TOKEN_SITE)

        self.tree_two_level = MultiLevelTree(process_names=[PROCESS_CLIENT_MONTHLY],
                                             full_name=TOKEN_CLIENT)

    def tearDown(self):
        del self.tree_four_level
        del self.tree_two_level

    def test_find_counterpart_for(self):
        self.tree_four_level.build_tree()
        self.tree_two_level.build_tree()

        actual_yearly_timeperiod = time_helper.actual_timeperiod(QUALIFIER_YEARLY)
        actual_monthly_timeperiod = time_helper.actual_timeperiod(QUALIFIER_MONTHLY)
        actual_daily_timeperiod = time_helper.actual_timeperiod(QUALIFIER_DAILY)
        actual_hourly_timeperiod = time_helper.actual_timeperiod(QUALIFIER_HOURLY)

        # case 1 - left-to-right successful comparison
        node_a = self.tree_two_level.get_node(PROCESS_CLIENT_MONTHLY, actual_monthly_timeperiod)
        node_b = node_a.find_counterpart_in(self.tree_four_level)
        self.assertIsNotNone(node_b)
        self.assertEqual(node_a.timeperiod, node_b.timeperiod)

        # case 2 - right-to-left successful comparison
        node_a = self.tree_four_level.get_node(PROCESS_SITE_MONTHLY, actual_monthly_timeperiod)
        node_b = node_a.find_counterpart_in(self.tree_two_level)
        self.assertIsNotNone(node_b)
        self.assertEqual(node_a.timeperiod, node_b.timeperiod)

        # case 3 - right-to-left unsuccessful comparison
        node_a = self.tree_four_level.get_node(PROCESS_SITE_DAILY, actual_daily_timeperiod)
        node_b = node_a.find_counterpart_in(self.tree_two_level)
        self.assertIsNone(node_b)

        # case 4 - right-to-left unsuccessful comparison
        node_a = self.tree_four_level.get_node(PROCESS_SITE_HOURLY, actual_hourly_timeperiod)
        node_b = node_a.find_counterpart_in(self.tree_two_level)
        self.assertIsNone(node_b)

        # case 5 - right-to-left unsuccessful comparison
        node_a = self.tree_four_level.get_node(PROCESS_SITE_YEARLY, actual_yearly_timeperiod)
        node_b = node_a.find_counterpart_in(self.tree_two_level)
        self.assertIsNone(node_b)


if __name__ == '__main__':
    unittest.main()
