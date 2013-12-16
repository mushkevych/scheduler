
__author__ = 'Bohdan Mushkevych'

import unittest
from tests import base_fixtures
from system import time_helper
from system.process_context import PROCESS_SITE_HOURLY, _TOKEN_SITE, ProcessContext
from scheduler.tree import TwoLevelTree
from settings import settings


class TestTwoLevelTree(unittest.TestCase):
    def setUp(self):
        self.initial_actual_timeperiod = time_helper.actual_timeperiod
        self.initial_synergy_start_time = settings['synergy_start_timeperiod']
        self.tree = TwoLevelTree(PROCESS_SITE_HOURLY, _TOKEN_SITE, 'some_mx_page')

    def tearDown(self):
        del self.tree
        settings['synergy_start_timeperiod'] = self.initial_synergy_start_time
        time_helper.actual_timeperiod = self.initial_actual_timeperiod

    def test_simple_build_tree(self):
        self.tree.build_tree()
        assert len(self.tree.root.children) == 1

        actual_timeperiod = time_helper.actual_timeperiod(ProcessContext.QUALIFIER_HOURLY)
        assert actual_timeperiod in self.tree.root.children

        assert self.tree.root.children[actual_timeperiod].timeperiod == actual_timeperiod

    def _perform_assertions(self, start_timeperiod, delta):
        assert len(self.tree.root.children) == delta + 1, 'Expected number of child nodes was %d, while actual is %d' \
                                                          % (delta + 1, len(self.tree.root.children))

        loop_timeperiod = start_timeperiod
        for _ in range(delta + 1):
            assert loop_timeperiod in self.tree.root.children
            assert self.tree.root.children[loop_timeperiod].timeperiod == loop_timeperiod
            loop_timeperiod = time_helper.increment_timeperiod(ProcessContext.QUALIFIER_HOURLY, loop_timeperiod)

    def test_less_simple_build_tree(self):
        delta = 5
        new_synergy_start_time = base_fixtures.wind_the_time(ProcessContext.QUALIFIER_HOURLY,
                                                             self.initial_synergy_start_time,
                                                             -delta)
        settings['synergy_start_timeperiod'] = new_synergy_start_time

        self.tree.build_tree()
        self._perform_assertions(new_synergy_start_time, delta)

    def test_catching_up_time_build_tree(self):
        delta = 5
        new_synergy_start_time = base_fixtures.wind_the_time(ProcessContext.QUALIFIER_HOURLY,
                                                             self.initial_synergy_start_time,
                                                             -delta)
        settings['synergy_start_timeperiod'] = new_synergy_start_time

        self.tree.build_tree()
        self._perform_assertions(new_synergy_start_time, delta)

        new_actual_timeperiod = base_fixtures.wind_the_time(ProcessContext.QUALIFIER_HOURLY,
                                                            self.initial_synergy_start_time,
                                                            delta)

        time_helper.actual_timeperiod = \
            base_fixtures.wind_actual_timeperiod(time_helper.synergy_to_datetime(ProcessContext.QUALIFIER_HOURLY,
                                                                                 new_actual_timeperiod))
        self.tree.build_tree()
        self._perform_assertions(new_synergy_start_time, 2 * delta)

    def test_is_managing_process(self):
        self.assertTrue(self.tree.is_managing_process(PROCESS_SITE_HOURLY))
        self.assertFalse(self.tree.is_managing_process('AnyOtherProcess'))


if __name__ == '__main__':
    unittest.main()
