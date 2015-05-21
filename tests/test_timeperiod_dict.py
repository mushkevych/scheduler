__author__ = 'Bohdan Mushkevych'

import unittest

from synergy.system import time_helper
from synergy.system.timeperiod_dict import TimeperiodDict
from synergy.system.time_qualifier import *


class TestTimeHelper(unittest.TestCase):
    def test_identity_translation(self):
        fixture = {
            QUALIFIER_HOURLY: {'2010123123': '2010123123',
                               '2010123110': '2010123110',
                               '2010123100': '2010123100'},
            QUALIFIER_DAILY: {'2010123100': '2010123100',
                              '2010120100': '2010120100'},
            QUALIFIER_MONTHLY: {'2010120000': '2010120000',
                                '2010010000': '2010010000'},
            QUALIFIER_YEARLY: {'2010000000': '2010000000',
                               '2011000000': '2011000000'},
        }

        for qualifier, f in fixture.items():
            d = TimeperiodDict(qualifier, 1)
            for key, value in f.items():
                self.assertEqual(d._translate_timeperiod(key), value,
                                 msg='failing combination: q={0} fixture={1}:{2}'.
                                 format(qualifier, key, value))

    def test_grouping_translation(self):
        fixture = {
            QUALIFIER_HOURLY: {'2010123123': '2010123123',
                               '2010123100': '2010123105'},
            QUALIFIER_DAILY: {'2010123100': '2010123100',
                              '2010120100': '2010120500',
                              '2010120600': '2010121000',
                              '2010123000': '2010123000',
                              '2010122000': '2010122000'},
            QUALIFIER_MONTHLY: {'2010120000': '2010120000',
                                '2010010000': '2010050000',
                                '2010100000': '2010100000',
                                '2010110000': '2010120000'},
            QUALIFIER_YEARLY: {'2010000000': '2010000000',
                               '2011000000': '2011000000'},
        }

        for qualifier, f in fixture.items():
            d = TimeperiodDict(qualifier, 5)
            for key, value in f.items():
                self.assertEqual(d._translate_timeperiod(key), value,
                                 msg='failing combination: q={0} fixture={1}/{2}'.
                                 format(qualifier, key, value))

    def test_mapping_methods(self):
        test_dict = TimeperiodDict(QUALIFIER_HOURLY, 3)
        timeperiod = '2010123100'
        for i in range(0, 24):
            test_dict[timeperiod] = i
            timeperiod = time_helper.increment_timeperiod(QUALIFIER_HOURLY, timeperiod)

        self.assertEqual(len(test_dict), 8)
        self.assertIn('2010123103', test_dict)
        self.assertIn('2010123106', test_dict)
        self.assertIn('2010123109', test_dict)
        self.assertIn('2010123112', test_dict)
        self.assertIn('2010123115', test_dict)
        self.assertIn('2010123118', test_dict)
        self.assertIn('2010123121', test_dict)
        self.assertIn('2010123123', test_dict)
        self.assertNotIn('2010123100', test_dict)
        self.assertNotIn('2010123101', test_dict)
        self.assertNotIn('2010123105', test_dict)

        # get method
        self.assertIsNone(test_dict.get('2010123105'))
        self.assertIsNotNone(test_dict.get('2010123106'))

        counter = 0
        for _ in test_dict:
            counter += 1
        self.assertEqual(counter, 8)


if __name__ == '__main__':
    unittest.main()
