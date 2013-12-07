__author__ = 'Bohdan Mushkevych'

import unittest
from datetime import datetime
from system import time_helper
from system.process_context import ProcessContext


class TestTimeHelper(unittest.TestCase):
    def test_raw_to_session(self):
        timestamp = 1304618357.4482391
        expected_output = '20110505175917'
        assert time_helper.raw_to_session(timestamp) == expected_output

    def test_session_to_hour(self):
        timestamp = '20110101161633'
        expected_output = '2011010116'
        assert time_helper.session_to_hour(timestamp) == expected_output

    def test_hour_to_day(self):
        timeperiod = '2011010116'
        expected_output = '2011010100'
        assert time_helper.hour_to_day(timeperiod) == expected_output

    def test_day_to_month(self):
        timeperiod = '2011010100'
        expected_output = '2011010000'
        assert time_helper.day_to_month(timeperiod) == expected_output

    def test_month_to_year(self):
        timeperiod = '2011010000'
        expected_output = '2011000000'
        assert time_helper.month_to_year(timeperiod) == expected_output

        timeperiod = '2011120000'
        expected_output = '2011000000'
        assert time_helper.month_to_year(timeperiod) == expected_output

    def test_cast_to_time_qualifier(self):
        qualifiers = [ProcessContext.QUALIFIER_HOURLY, ProcessContext.QUALIFIER_HOURLY,
                     ProcessContext.QUALIFIER_DAILY, ProcessContext.QUALIFIER_DAILY,
                     ProcessContext.QUALIFIER_MONTHLY, ProcessContext.QUALIFIER_MONTHLY,
                     ProcessContext.QUALIFIER_YEARLY, ProcessContext.QUALIFIER_YEARLY]
        params = ['2010123123', '20101231231232',
                  '2010123123', '2010123100',
                  '2010123100', '2010120000',
                  '2010120000', '2010000000']
        expected = ['2010123123', '2010123123',
                    '2010123100', '2010123100',
                    '2010120000', '2010120000',
                    '2010000000', '2010000000']
        for i in range(8):
            assert time_helper.cast_to_time_qualifier(qualifiers[i], params[i]) == expected[i]

    def test_datetime_to_synergy(self):
        dt = datetime(year=2010, month=12, day=31, hour=23, minute=50, second=15)

        qualifiers = [ProcessContext.QUALIFIER_REAL_TIME,
                      ProcessContext.QUALIFIER_HOURLY,
                      ProcessContext.QUALIFIER_DAILY,
                      ProcessContext.QUALIFIER_MONTHLY,
                      ProcessContext.QUALIFIER_YEARLY]

        expected = ['20101231235015',
                    '2010123123',
                    '2010123100',
                    '2010120000',
                    '2010000000']

        for i in range(4):
            assert time_helper.datetime_to_synergy(qualifiers[i], dt) == expected[i]

    def test_synergy_to_datetime(self):
        qualifiers = [ProcessContext.QUALIFIER_REAL_TIME,
                      ProcessContext.QUALIFIER_HOURLY,
                      ProcessContext.QUALIFIER_DAILY,
                      ProcessContext.QUALIFIER_MONTHLY,
                      ProcessContext.QUALIFIER_YEARLY]

        expected = [datetime(year=2010, month=12, day=31, hour=23, minute=12, second=34),
                    datetime(year=2010, month=12, day=31, hour=23, minute=00, second=0),
                    datetime(year=2010, month=12, day=31, hour=00, minute=00, second=0),
                    datetime(year=2010, month=12, day=01, hour=00, minute=00, second=0),
                    datetime(year=2010, month=01, day=01, hour=00, minute=00, second=0)]

        params = ['20101231231234',
                  '2010123123',
                  '2010123100',
                  '2010120000',
                  '2010000000']

        for i in range(5):
            assert time_helper.synergy_to_datetime(qualifiers[i], params[i]) == expected[i]

    def test_increment_time(self):
        stamps = ['2011010100', '2011010112', '2011010123']
        expected = ['2011010101', '2011010113', '2011010200']
        for i in range(3):
            assert time_helper.increment_timeperiod(ProcessContext.QUALIFIER_HOURLY, stamps[i]) == expected[i]

        stamps = ['2011010100', '2011013100', '2010123100']
        expected = ['2011010200', '2011020100', '2011010100']
        for i in range(3):
            assert time_helper.increment_timeperiod(ProcessContext.QUALIFIER_DAILY, stamps[i]) == expected[i]

        stamps = ['2011010000', '2011120000', '2011100000']
        expected = ['2011020000', '2012010000', '2011110000']
        for i in range(3):
            assert time_helper.increment_timeperiod(ProcessContext.QUALIFIER_MONTHLY, stamps[i]) == expected[i]

        stamps = ['2011000000', '2012000000', '2099000000']
        expected = ['2012000000', '2013000000', '2100000000']
        for i in range(3):
            assert time_helper.increment_timeperiod(ProcessContext.QUALIFIER_YEARLY, stamps[i]) == expected[i]

    def test_duration_calculation(self):
        timestamp_1 = 1305934600.0
        timestamp_2 = 1305934630.0
        expected_output_1 = '20110520233640'
        expected_output_2 = '20110520233710'

        assert time_helper.raw_to_session(timestamp_1) == expected_output_1
        assert time_helper.raw_to_session(timestamp_2) == expected_output_2

        assert time_helper.session_to_epoch(expected_output_1) == timestamp_1
        assert time_helper.session_to_epoch(expected_output_2) == timestamp_2

        duration = timestamp_2 - time_helper.session_to_epoch(expected_output_1)
        assert duration == timestamp_2 - timestamp_1


if __name__ == '__main__':
    unittest.main()
