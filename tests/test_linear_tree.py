__author__ = 'Bohdan Mushkevych'

import unittest

from tests import base_fixtures
from synergy.system import time_helper
from synergy.system.time_qualifier import QUALIFIER_HOURLY
from constants import PROCESS_SITE_HOURLY, TOKEN_SITE
from synergy.scheduler.tree_node import LinearNode
from synergy.scheduler.tree import MultiLevelTree
from synergy.conf import settings


class TestLinearTree(unittest.TestCase):
    def setUp(self):
        self.initial_actual_timeperiod = time_helper.actual_timeperiod
        self.initial_synergy_start_time = settings.settings['synergy_start_timeperiod']

        self.actual_timeperiod = time_helper.actual_timeperiod(QUALIFIER_HOURLY)
        settings.settings['synergy_start_timeperiod'] = self.actual_timeperiod

        self.tree = MultiLevelTree(process_names=[PROCESS_SITE_HOURLY],
                                   node_klass=LinearNode, mx_name=TOKEN_SITE, mx_page='some_mx_page')

    def tearDown(self):
        del self.tree
        settings.settings['synergy_start_timeperiod'] = self.initial_synergy_start_time
        time_helper.actual_timeperiod = self.initial_actual_timeperiod

    def test_simple_build_tree(self):
        self.tree.build_tree()
        self.assertEqual(len(self.tree.root.children), 1)

        actual_timeperiod = time_helper.actual_timeperiod(QUALIFIER_HOURLY)
        self.assertIn(actual_timeperiod, self.tree.root.children)
        self.assertEqual(self.tree.root.children[actual_timeperiod].timeperiod, actual_timeperiod)

    def _perform_assertions(self, start_timeperiod, delta):
        self.assertEqual(len(self.tree.root.children), delta + 1,
                         'Expected number of child nodes was %d, while actual is %d'
                         % (delta + 1, len(self.tree.root.children)))

        loop_timeperiod = start_timeperiod
        for _ in range(delta + 1):
            self.assertIn(loop_timeperiod, self.tree.root.children)
            self.assertEqual(self.tree.root.children[loop_timeperiod].timeperiod, loop_timeperiod)
            loop_timeperiod = time_helper.increment_timeperiod(QUALIFIER_HOURLY, loop_timeperiod)

    def test_less_simple_build_tree(self):
        delta = 5
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
