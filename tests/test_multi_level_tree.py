__author__ = 'Bohdan Mushkevych'

import unittest

from tests import base_fixtures
from constants import PROCESS_SITE_HOURLY, PROCESS_SITE_DAILY, TOKEN_SITE, PROCESS_SITE_YEARLY, PROCESS_SITE_MONTHLY
from synergy.system import time_helper
from synergy.system.utils import increment_family_property
from synergy.system.time_qualifier import QUALIFIER_HOURLY
from synergy.scheduler.tree_node import TreeNode
from synergy.scheduler.tree import MultiLevelTree
from synergy.conf import settings


class TestTwoLevelTree(unittest.TestCase):
    def setUp(self):
        self.initial_actual_timeperiod = time_helper.actual_timeperiod
        self.initial_synergy_start_time = settings.settings['synergy_start_timeperiod']

        self.actual_timeperiod = time_helper.actual_timeperiod(QUALIFIER_HOURLY)
        settings.settings['synergy_start_timeperiod'] = self.actual_timeperiod

        self.trees = []
        self.trees.append(MultiLevelTree(process_names=[PROCESS_SITE_YEARLY, PROCESS_SITE_MONTHLY,
                                                        PROCESS_SITE_DAILY, PROCESS_SITE_HOURLY],
                                         mx_name=TOKEN_SITE, mx_page='some_mx_page'))
        self.trees.append(MultiLevelTree(process_names=[PROCESS_SITE_HOURLY, PROCESS_SITE_DAILY],
                                         mx_name=TOKEN_SITE, mx_page='some_mx_page'))
        self.trees.append(MultiLevelTree(process_names=[PROCESS_SITE_YEARLY, PROCESS_SITE_MONTHLY,
                                                        PROCESS_SITE_DAILY],
                                         mx_name=TOKEN_SITE, mx_page='some_mx_page'))
        self.trees.append(MultiLevelTree(process_names=[PROCESS_SITE_HOURLY],
                                         mx_name=TOKEN_SITE, mx_page='some_mx_page'))

    def tearDown(self):
        del self.trees
        settings.settings['synergy_start_timeperiod'] = self.initial_synergy_start_time
        time_helper.actual_timeperiod = self.initial_actual_timeperiod

    def test_simple_build_tree(self):
        visited_nodes = set()
        nodes_per_process = dict()

        def calculate_nodes_per_process(tree_node):
            assert isinstance(tree_node, TreeNode)
            if tree_node.timeperiod in visited_nodes:
                # node is already calculated
                return

            visited_nodes.add(tree_node.timeperiod)
            increment_family_property(tree_node.process_name, nodes_per_process)
            for child_timeperiod, child_node in tree_node.children.items():
                calculate_nodes_per_process(child_node)

        for tree in self.trees:
            assert isinstance(tree, MultiLevelTree)
            tree.build_tree()
            calculate_nodes_per_process(tree.root)

            self.assertEqual(len(nodes_per_process), tree.process_hierarchy.entries)
            for process_name, counter in nodes_per_process.items():
                self.assertEqual(counter, 1)

    def _perform_assertions(self, tree, delta):
        def calculate_leafs(tree_node):
            assert isinstance(tree_node, TreeNode)
            if not tree_node.children:
                # node is the leaf
                return 1

            counter = 0
            for child_timeperiod, child_node in tree_node.children.items():
                counter += calculate_leafs(child_node)
            return counter

        number_of_leafs = calculate_leafs(tree.root)
        self.assertEqual(number_of_leafs, delta + 1, 'Expected number of leaf nodes was %d, while actual is %d'
                                                     % (delta + 1, number_of_leafs))

    def test_less_simple_build_tree(self):
        delta = 105
        new_synergy_start_time = base_fixtures.wind_the_time(QUALIFIER_HOURLY,
                                                             self.actual_timeperiod,
                                                             -delta)
        settings.settings['synergy_start_timeperiod'] = new_synergy_start_time

        for tree in self.trees:
            assert isinstance(tree, MultiLevelTree)
            tree.build_tree()
            self._perform_assertions(new_synergy_start_time, delta)

    def test_catching_up_time_build_tree(self):
        delta = 5
        new_synergy_start_time = base_fixtures.wind_the_time(QUALIFIER_HOURLY,
                                                             self.actual_timeperiod,
                                                             -delta)
        settings.settings['synergy_start_timeperiod'] = new_synergy_start_time

        for tree in self.trees:
            assert isinstance(tree, MultiLevelTree)
            tree.build_tree()
            self._perform_assertions(new_synergy_start_time, delta)

        new_actual_timeperiod = base_fixtures.wind_the_time(QUALIFIER_HOURLY,
                                                            self.actual_timeperiod,
                                                            delta)

        time_helper.actual_timeperiod = \
            base_fixtures.wind_actual_timeperiod(time_helper.synergy_to_datetime(QUALIFIER_HOURLY,
                                                                                 new_actual_timeperiod))
        for tree in self.trees:
            assert isinstance(tree, MultiLevelTree)
            tree.build_tree()
            self._perform_assertions(new_synergy_start_time, 2 * delta)


if __name__ == '__main__':
    unittest.main()
