__author__ = 'Bohdan Mushkevych'

import unittest
from collections import OrderedDict

from synergy.system import time_helper
from synergy.system.timeperiod_dict import TimeperiodDict
from synergy.system.time_qualifier import *


class TestTimeperiodDict(unittest.TestCase):
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
                                 msg='failing combination: q={0} fixture={1}:{2} actual={3}'.
                                 format(qualifier, key, value, d._translate_timeperiod(key)))

    def test_grouping_translation(self):
        fixture = {
            QUALIFIER_HOURLY: {'2010123123': '2010123123',
                               '2010123122': '2010123123',
                               '2010123110': '2010123110',
                               '2010123111': '2010123115',
                               '2010123101': '2010123105',
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
        }

        for qualifier, f in fixture.items():
            d = TimeperiodDict(qualifier, 5)
            for key, value in f.items():
                self.assertEqual(d._translate_timeperiod(key), value,
                                 msg='failing combination: q={0} fixture={1}/{2} actual={3}'.
                                 format(qualifier, key, value, d._translate_timeperiod(key)))

        fixture = {
            QUALIFIER_YEARLY: {'2010000000': '2010000000',
                               '2011000000': '2011000000'}
        }
        for qualifier, f in fixture.items():
            try:
                _ = TimeperiodDict(qualifier, 5)
                self.assertTrue(False, 'YEARLY should allow only identity grouping (i.e. grouping=1)')
            except AssertionError:
                self.assertTrue(True)

    def test_container_methods(self):
        test_dict = TimeperiodDict(QUALIFIER_HOURLY, 3)
        timeperiod = '2010123100'
        for i in range(0, 24):
            test_dict[timeperiod] = i
            timeperiod = time_helper.increment_timeperiod(QUALIFIER_HOURLY, timeperiod)

        fixture = OrderedDict()
        fixture[(0, 4)] = 3
        fixture[(4, 7)] = 6
        fixture[(7, 10)] = 9
        fixture[(10, 13)] = 12
        fixture[(13, 16)] = 15
        fixture[(16, 19)] = 18
        fixture[(19, 22)] = 21
        fixture[(22, 24)] = 23

        timeperiod = '2010123100'
        for boundaries, value in fixture.items():
            lower_boundary, upper_boundary = boundaries
            for i in range(lower_boundary, upper_boundary):
                self.assertEqual(test_dict[timeperiod], value,
                                 msg='failing combination: timeperiod={0} i={1} actual/expected={2}/{3}'.
                                 format(timeperiod, i, test_dict[timeperiod], value))
                # get method
                self.assertIsNotNone(test_dict.get(timeperiod), )

                timeperiod = time_helper.increment_timeperiod(QUALIFIER_HOURLY, timeperiod)

        # test __len__ method
        self.assertEqual(len(test_dict), 8)

        # test __iter__ method
        counter = 0
        for _ in test_dict:
            counter += 1
        self.assertEqual(counter, 8)


if __name__ == '__main__':
    unittest.main()
