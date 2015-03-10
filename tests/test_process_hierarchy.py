__author__ = 'Bohdan Mushkevych'

import unittest

from synergy.system.time_qualifier import *
from context import PROCESS_SITE_HOURLY, PROCESS_SITE_DAILY, PROCESS_SITE_MONTHLY, PROCESS_SITE_YEARLY
from synergy.conf import context
from synergy.scheduler.process_hierarchy import Hierarchy


class TestProcessHierarchy(unittest.TestCase):
    def _perform_assertions(self, hierarchy, process_name_desc, time_qualifier_desc):
        index = 0
        for hierarchy_key in hierarchy:
            process_name = process_name_desc[index]
            index += 1
            self.assertEqual(hierarchy[hierarchy_key].process_entry.process_name, process_name)

        for process_name in process_name_desc:
            self.assertIn(process_name, hierarchy)

        for qualifier in time_qualifier_desc:
            self.assertTrue(hierarchy.has_qualifier(qualifier))

    def test_four_level(self):
        hourly = context.process_context[PROCESS_SITE_HOURLY]
        daily = context.process_context[PROCESS_SITE_DAILY]
        monthly = context.process_context[PROCESS_SITE_MONTHLY]
        yearly = context.process_context[PROCESS_SITE_YEARLY]
        hierarchy = Hierarchy(hourly, yearly, monthly, daily)

        process_name_desc = [PROCESS_SITE_YEARLY, PROCESS_SITE_MONTHLY, PROCESS_SITE_DAILY, PROCESS_SITE_HOURLY]
        time_qualifier_desc = [QUALIFIER_YEARLY, QUALIFIER_MONTHLY, QUALIFIER_DAILY, QUALIFIER_HOURLY]

        self._perform_assertions(hierarchy, process_name_desc, time_qualifier_desc)

    def test_three_level(self):
        hourly = context.process_context[PROCESS_SITE_HOURLY]
        daily = context.process_context[PROCESS_SITE_DAILY]
        monthly = context.process_context[PROCESS_SITE_MONTHLY]
        hierarchy = Hierarchy(hourly, monthly, daily)

        process_name_desc = [PROCESS_SITE_MONTHLY, PROCESS_SITE_DAILY, PROCESS_SITE_HOURLY]
        time_qualifier_desc = [QUALIFIER_MONTHLY, QUALIFIER_DAILY, QUALIFIER_HOURLY]

        self._perform_assertions(hierarchy, process_name_desc, time_qualifier_desc)

    def test_two_level(self):
        hourly = context.process_context[PROCESS_SITE_HOURLY]
        daily = context.process_context[PROCESS_SITE_DAILY]
        hierarchy = Hierarchy(hourly, daily)

        process_name_desc = [PROCESS_SITE_DAILY, PROCESS_SITE_HOURLY]
        time_qualifier_desc = [QUALIFIER_DAILY, QUALIFIER_HOURLY]

        self._perform_assertions(hierarchy, process_name_desc, time_qualifier_desc)

    def test_one_level(self):
        daily = context.process_context[PROCESS_SITE_DAILY]
        hierarchy = Hierarchy(daily)

        process_name_desc = [PROCESS_SITE_DAILY]
        time_qualifier_desc = [QUALIFIER_DAILY]

        self._perform_assertions(hierarchy, process_name_desc, time_qualifier_desc)

    def test_mix(self):
        hourly = context.process_context[PROCESS_SITE_HOURLY]
        yearly = context.process_context[PROCESS_SITE_YEARLY]
        hierarchy = Hierarchy(hourly, yearly)

        process_name_desc = [PROCESS_SITE_YEARLY, PROCESS_SITE_HOURLY]
        time_qualifier_desc = [QUALIFIER_YEARLY, QUALIFIER_HOURLY]

        self._perform_assertions(hierarchy, process_name_desc, time_qualifier_desc)


if __name__ == '__main__':
    unittest.main()
