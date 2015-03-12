__author__ = 'Bohdan Mushkevych'

import unittest

from synergy.system.time_qualifier import *
from context import PROCESS_SITE_HOURLY, PROCESS_SITE_DAILY, PROCESS_SITE_MONTHLY, PROCESS_SITE_YEARLY, \
    PROCESS_BASH_DRIVER
from synergy.scheduler.process_hierarchy import ProcessHierarchy


class TestProcessHierarchy(unittest.TestCase):
    def _perform_assertions(self, hierarchy, process_name_desc, time_qualifier_desc,
                            top_process_name, bottom_process_name):
        index = 0
        for hierarchy_key in hierarchy:
            process_name = process_name_desc[index]
            index += 1
            self.assertEqual(hierarchy[hierarchy_key].process_entry.process_name, process_name)

        self.assertEqual(hierarchy.top_process.process_name, top_process_name)
        self.assertEqual(hierarchy.bottom_process.process_name, bottom_process_name)

        for process_name in process_name_desc:
            self.assertIn(process_name, hierarchy)

        for qualifier in time_qualifier_desc:
            self.assertTrue(hierarchy.has_qualifier(qualifier))

    def test_four_level(self):
        hierarchy = ProcessHierarchy(PROCESS_SITE_HOURLY, PROCESS_SITE_YEARLY, PROCESS_SITE_MONTHLY, PROCESS_SITE_DAILY)

        process_name_desc = [PROCESS_SITE_YEARLY, PROCESS_SITE_MONTHLY, PROCESS_SITE_DAILY, PROCESS_SITE_HOURLY]
        time_qualifier_desc = [QUALIFIER_YEARLY, QUALIFIER_MONTHLY, QUALIFIER_DAILY, QUALIFIER_HOURLY]

        self._perform_assertions(hierarchy, process_name_desc, time_qualifier_desc,
                                 PROCESS_SITE_YEARLY, PROCESS_SITE_HOURLY)

    def test_three_level(self):
        hierarchy = ProcessHierarchy(PROCESS_SITE_HOURLY, PROCESS_SITE_MONTHLY, PROCESS_SITE_DAILY)

        process_name_desc = [PROCESS_SITE_MONTHLY, PROCESS_SITE_DAILY, PROCESS_SITE_HOURLY]
        time_qualifier_desc = [QUALIFIER_MONTHLY, QUALIFIER_DAILY, QUALIFIER_HOURLY]

        self._perform_assertions(hierarchy, process_name_desc, time_qualifier_desc,
                                 PROCESS_SITE_MONTHLY, PROCESS_SITE_HOURLY)

    def test_two_level(self):
        hierarchy = ProcessHierarchy(PROCESS_SITE_HOURLY, PROCESS_SITE_DAILY)

        process_name_desc = [PROCESS_SITE_DAILY, PROCESS_SITE_HOURLY]
        time_qualifier_desc = [QUALIFIER_DAILY, QUALIFIER_HOURLY]

        self._perform_assertions(hierarchy, process_name_desc, time_qualifier_desc,
                                 PROCESS_SITE_DAILY, PROCESS_SITE_HOURLY)

    def test_one_level(self):
        hierarchy = ProcessHierarchy(PROCESS_SITE_DAILY)

        process_name_desc = [PROCESS_SITE_DAILY]
        time_qualifier_desc = [QUALIFIER_DAILY]

        self._perform_assertions(hierarchy, process_name_desc, time_qualifier_desc,
                                 PROCESS_SITE_DAILY, PROCESS_SITE_DAILY)

    def test_mix(self):
        hierarchy = ProcessHierarchy(PROCESS_SITE_HOURLY, PROCESS_SITE_YEARLY)

        process_name_desc = [PROCESS_SITE_YEARLY, PROCESS_SITE_HOURLY]
        time_qualifier_desc = [QUALIFIER_YEARLY, QUALIFIER_HOURLY]

        self._perform_assertions(hierarchy, process_name_desc, time_qualifier_desc,
                                 PROCESS_SITE_YEARLY, PROCESS_SITE_HOURLY)

    def test_invalid_process_name(self):
        try:
            ProcessHierarchy(PROCESS_BASH_DRIVER, PROCESS_SITE_HOURLY)
            self.assertTrue(False, 'AttributeError should have been thrown for improper hierarchical process')
        except AttributeError:
            self.assertTrue(True, 'AttributeError was expected and caught')


if __name__ == '__main__':
    unittest.main()
