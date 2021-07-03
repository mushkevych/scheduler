__author__ = 'Bohdan Mushkevych'

import unittest
from datetime import datetime

from synergy.system import time_helper
from synergy.system.time_qualifier import *


class TestTimeHelper(unittest.TestCase):
    def test_raw_to_session(self):
        timestamp = 1304618357.4482391
        expected_output = '20110505175917'
        self.assertEqual(time_helper.raw_to_session(timestamp), expected_output)

    def test_session_to_hour(self):
        timestamp = '20110101161633'
        expected_output = '2011010116'
        self.assertEqual(time_helper.session_to_hour(timestamp), expected_output)

    def test_hour_to_day(self):
        timeperiod = '2011010116'
        expected_output = '2011010100'
        self.assertEqual(time_helper.hour_to_day(timeperiod), expected_output)

    def test_day_to_month(self):
        timeperiod = '2011010100'
        expected_output = '2011010000'
        self.assertEqual(time_helper.day_to_month(timeperiod), expected_output)

    def test_month_to_year(self):
        timeperiod = '2011010000'
        expected_output = '2011000000'
        self.assertEqual(time_helper.month_to_year(timeperiod), expected_output)

        timeperiod = '2011120000'
        expected_output = '2011000000'
        self.assertEqual(time_helper.month_to_year(timeperiod), expected_output)

    def test_cast_to_time_qualifier(self):
        fixture = {
            QUALIFIER_HOURLY: [('2010123123', '20101231231232'),
                               ('2010123123', '2010123123')],
            QUALIFIER_DAILY: [('2010123123', '2010123100', '20101231231232'),
                              ('2010123100', '2010123100', '2010123100')],
            QUALIFIER_MONTHLY: [('2010123100', '2010120000', '20101231231232'),
                                ('2010120000', '2010120000', '2010120000')],
            QUALIFIER_YEARLY: [('2010120000', '2010000000', '20101231231232'),
                               ('2010000000', '2010000000', '2010000000')],
            QUALIFIER_ONCE: [('2010120000', '2010000000', '20101231231232'),
                             ('1111111111', '1111111111', '1111111111')]
        }

        for key, test_set in fixture.items():
            for idx, value in enumerate(test_set[0]):
                test_value = test_set[0][idx]
                expected_value = test_set[1][idx]
                self.assertEqual(time_helper.cast_to_time_qualifier(key, test_value), expected_value)

    def test_datetime_to_synergy(self):
        dt = datetime(year=2010, month=12, day=31, hour=23, minute=50, second=15)

        fixture = {QUALIFIER_REAL_TIME: '20101231235015',
                   QUALIFIER_HOURLY: '2010123123',
                   QUALIFIER_DAILY: '2010123100',
                   QUALIFIER_MONTHLY: '2010120000',
                   QUALIFIER_YEARLY: '2010000000',
                   QUALIFIER_ONCE: '1111111111'}

        for key, value in fixture.items():
            self.assertEqual(time_helper.datetime_to_synergy(key, dt), value)

    def test_synergy_to_datetime(self):
        fixture = {
            QUALIFIER_REAL_TIME: ['20101231231234',
                                  datetime(year=2010, month=12, day=31, hour=23, minute=12, second=34)],
            QUALIFIER_HOURLY: ['2010123123',
                               datetime(year=2010, month=12, day=31, hour=23, minute=0, second=0)],
            QUALIFIER_DAILY: ['2010123100',
                              datetime(year=2010, month=12, day=31, hour=0, minute=0, second=0)],
            QUALIFIER_MONTHLY: ['2010120000',
                                datetime(year=2010, month=12, day=1, hour=0, minute=0, second=0)],
            QUALIFIER_YEARLY: ['2010000000',
                               datetime(year=2010, month=1, day=1, hour=0, minute=0, second=0)],
            QUALIFIER_ONCE: ['2010000000',
                             datetime(year=1111, month=11, day=11, hour=11, minute=0, second=0)]
        }

        for qualifier, value in fixture.items():
            self.assertEqual(time_helper.synergy_to_datetime(qualifier, value[0]), value[1])

    def test_increment_time(self):
        stamps = ['2011010100', '2011010112', '2011010123']
        expected = ['2011010101', '2011010113', '2011010200']
        for idx, stamp in enumerate(stamps):
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_HOURLY, stamp), expected[idx])

        stamps = ['2011010100', '2011013100', '2010123100']
        expected = ['2011010200', '2011020100', '2011010100']
        for idx, stamp in enumerate(stamps):
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_DAILY, stamp), expected[idx])

        stamps = ['2011010000', '2011120000', '2011100000']
        expected = ['2011020000', '2012010000', '2011110000']
        for idx, stamp in enumerate(stamps):
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_MONTHLY, stamp), expected[idx])

        stamps = ['2011000000', '2012000000', '2099000000']
        expected = ['2012000000', '2013000000', '2100000000']
        for idx, stamp in enumerate(stamps):
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_YEARLY, stamp), expected[idx])

        stamps = ['2011000000', '2012000000', '2099000000', '1111111111']
        expected = ['1111111111', '1111111111', '1111111111', '1111111111']
        for idx, stamp in enumerate(stamps):
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_ONCE, stamp), expected[idx])

    def test_shift_time_by_delta(self):
        stamps = ['2011010100', '2011010112', '2011010123']
        expected = ['2011010103', '2011010115', '2011010202']
        for idx, stamp in enumerate(stamps):
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_HOURLY, stamp, delta=3), expected[idx])

        stamps = ['2011010100', '2011010112', '2011010123']
        expected = ['2010123121', '2011010109', '2011010120']
        for idx, stamp in enumerate(stamps):
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_HOURLY, stamp, delta=-3), expected[idx])

        stamps = ['2011010100', '2011013100', '2010123100']
        expected = ['2011010400', '2011020300', '2011010300']
        for idx, stamp in enumerate(stamps):
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_DAILY, stamp, delta=3), expected[idx])

        stamps = ['2011010100', '2011013100', '2010123100']
        expected = ['2010122900', '2011012800', '2010122800']
        for idx, stamp in enumerate(stamps):
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_DAILY, stamp, delta=-3), expected[idx])

        stamps = ['2011010000', '2011090000', '2010120000']
        expected = ['2011040000', '2011120000', '2011030000']
        for idx, stamp in enumerate(stamps):
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_MONTHLY, stamp, delta=3), expected[idx])

        stamps = ['2011010000', '2011090000', '2010120000']
        expected = ['2010100000', '2011060000', '2010090000']
        for idx, stamp in enumerate(stamps):
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_MONTHLY, stamp, delta=-3), expected[idx])

        stamps = ['2011010000', '2011090000', '2010120000']
        expected = ['2013020000', '2013100000', '2013010000']
        for idx, stamp in enumerate(stamps):
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_MONTHLY, stamp, delta=25), expected[idx])

        stamps = ['2011010000', '2011090000', '2010120000']
        expected = ['2008120000', '2009080000', '2008110000']
        for idx, stamp in enumerate(stamps):
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_MONTHLY, stamp, delta=-25), expected[idx])

        stamps = ['2011010000', '2011120000', '2011100000']
        expected = ['2010120000', '2011110000', '2011090000']
        for idx, stamp in enumerate(stamps):
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_MONTHLY, stamp, delta=-1),
                             expected[idx])

        stamps = ['2011000000', '2012000000', '2099000000']
        expected = ['2016000000', '2017000000', '2104000000']
        for idx, stamp in enumerate(stamps):
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_YEARLY, stamp, delta=5), expected[idx])

        stamps = ['2011000000', '2012000000', '2099000000']
        expected = ['2006000000', '2007000000', '2094000000']
        for idx, stamp in enumerate(stamps):
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_YEARLY, stamp, delta=-5), expected[idx])

        stamps = ['2011000000', '2012000000', '2099000000', '1111111111']
        expected = ['1111111111', '1111111111', '1111111111', '1111111111']
        for idx, stamp in enumerate(stamps):
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_ONCE, stamp, delta=-5), expected[idx])
            self.assertEqual(time_helper.increment_timeperiod(QUALIFIER_ONCE, stamp, delta=+5), expected[idx])

    def test_duration_calculation(self):
        timestamp_1 = 1305934600.0
        timestamp_2 = 1305934630.0
        expected_output_1 = '20110520233640'
        expected_output_2 = '20110520233710'

        self.assertEqual(time_helper.raw_to_session(timestamp_1), expected_output_1)
        self.assertEqual(time_helper.raw_to_session(timestamp_2), expected_output_2)

        self.assertEqual(time_helper.session_to_epoch(expected_output_1), timestamp_1)
        self.assertEqual(time_helper.session_to_epoch(expected_output_2), timestamp_2)

        duration = timestamp_2 - time_helper.session_to_epoch(expected_output_1)
        self.assertEqual(duration, timestamp_2 - timestamp_1)

    def test_tokenize_timeperiod(self):
        fixture = {QUALIFIER_HOURLY: ['2010123123', ('2010', '12', '31', '23')],
                   QUALIFIER_DAILY: ['2010123100', ('2010', '12', '31', '00')],
                   QUALIFIER_MONTHLY: ['2010120000', ('2010', '12', '00', '00')],
                   QUALIFIER_YEARLY: ['2010000000', ('2010', '00', '00', '00')],
                   QUALIFIER_ONCE: ['1111111111', ('1111', '11', '11', '11')]}

        for key, value in fixture.items():
            self.assertEqual(time_helper.tokenize_timeperiod(value[0]), value[1])


if __name__ == '__main__':
    unittest.main()
