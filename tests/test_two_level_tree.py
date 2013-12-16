__author__ = 'Bohdan Mushkevych'

import unittest
from datetime import datetime, timedelta
from system.process_context import PROCESS_SITE_HOURLY, _TOKEN_SITE, ProcessContext
from system import time_helper
from scheduler.tree import TwoLevelTree
from settings import settings


def wind_actual_timeperiod(new_time):
    def actual_timeperiod(time_qualifier):
        return time_helper.datetime_to_synergy(time_qualifier, new_time)

    return actual_timeperiod


def wind_the_time(time_qualifier, timeperiod, delta):
    pattern = time_helper.define_pattern(timeperiod)
    t = datetime.strptime(timeperiod, pattern)

    if time_qualifier == ProcessContext.QUALIFIER_HOURLY:
        t = t + timedelta(hours=delta)
        return t.strftime('%Y%m%d%H')
    elif time_qualifier == ProcessContext.QUALIFIER_DAILY:
        t = t + timedelta(days=delta)
        return t.strftime('%Y%m%d00')
    raise ValueError('unsupported time_qualifier')


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

    def test_less_simple_build_tree(self):
        delta = 5
        new_synergy_start_time = wind_the_time(ProcessContext.QUALIFIER_HOURLY,
                                               self.initial_synergy_start_time,
                                               -delta)
        settings['synergy_start_timeperiod'] = new_synergy_start_time

        self.tree.build_tree()
        assert len(self.tree.root.children) == delta + 1, 'Expected number of child nodes was %d, while actual is %d' \
                                                         % (delta + 1, len(self.tree.root.children))

        loop_timeperiod = new_synergy_start_time
        for _ in range(delta + 1):
            assert loop_timeperiod in self.tree.root.children
            assert self.tree.root.children[loop_timeperiod].timeperiod == loop_timeperiod
            loop_timeperiod = time_helper.increment_timeperiod(ProcessContext.QUALIFIER_HOURLY, loop_timeperiod)

    def test_catching_up_time_build_tree(self):
        delta = 5
        new_synergy_start_time = wind_the_time(ProcessContext.QUALIFIER_HOURLY,
                                               self.initial_synergy_start_time,
                                               -delta)
        settings['synergy_start_timeperiod'] = new_synergy_start_time

        self.tree.build_tree()
        new_actual_timeperiod = wind_the_time(ProcessContext.QUALIFIER_HOURLY,
                                              self.initial_synergy_start_time,
                                              delta)

        time_helper.actual_timeperiod = \
            wind_actual_timeperiod(time_helper.synergy_to_datetime(ProcessContext.QUALIFIER_HOURLY,
                                                                   new_actual_timeperiod))
        self.tree.build_tree()

        assert len(self.tree.root.children) == 2 * delta + 1, 'Expected number of child nodes was %d, while actual is %d' \
                                                              % (2 * delta + 1, len(self.tree.root.children))

        loop_timeperiod = new_synergy_start_time
        for _ in range(2 * delta + 1):
            assert loop_timeperiod in self.tree.root.children
            assert self.tree.root.children[loop_timeperiod].timeperiod == loop_timeperiod
            loop_timeperiod = time_helper.increment_timeperiod(ProcessContext.QUALIFIER_HOURLY, loop_timeperiod)

    def test_is_managing_process(self):
        self.assertTrue(self.tree.is_managing_process(PROCESS_SITE_HOURLY))
        self.assertFalse(self.tree.is_managing_process('AnyOtherProcess'))


if __name__ == '__main__':
    unittest.main()
