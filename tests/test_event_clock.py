__author__ = 'Bohdan Mushkevych'

import unittest
from datetime import datetime, timedelta

from synergy.system.event_clock import EventClock, EventTime, parse_time_trigger_string, format_time_trigger_string
from synergy.system.repeat_timer import RepeatTimer


class TestEventClock(unittest.TestCase):
    def test_utc_now(self):
        utc_now = datetime.utcnow()
        self.obj = EventTime.utc_now()
        print str(self.obj)
        assert self.obj.day_of_week == str(utc_now.weekday()) \
               and self.obj.time_of_day.hour == utc_now.hour \
               and self.obj.time_of_day.minute == utc_now.minute \
               and self.obj.time_of_day.second == 0 \
               and self.obj.time_of_day.microsecond == 0

        other_obj = EventTime.utc_now()
        self.assertEqual(other_obj, self.obj)

    def test_eq(self):
        params = [EventTime(x) for x in ['17:00', '4-15:45', '*-09:00', '8:01']]
        expected = [EventTime(x) for x in ['*-17:00', '2-17:00', '4-15:45', '*-09:00', '*-9:00', '*-08:01']]
        not_expected = [EventTime(x) for x in ['*-17:15', '1-15:45', '*-9:01', '*-18:01']]

        for event in expected:
            self.assertIn(event, params)

        for event in not_expected:
            self.assertNotIn(event, params)

    def test_parser(self):
        fixture = {'every 300': (300, RepeatTimer),
                   'every  500': (500, RepeatTimer),
                   'every 1': (1, RepeatTimer),
                   'at *-17:00,  4-15:45,*-09:00 ': (['*-17:00', '4-15:45', '*-09:00'], EventClock),
                   'at 5-18:00  ,4-18:05 ,1-9:01 ': (['5-18:00', '4-18:05', '1-9:01'], EventClock),
                   'at *-08:01': (['*-08:01'], EventClock),
                   'at 8:30': (['8:30'], EventClock)}

        for line, expected_output in fixture.iteritems():
            processed_tuple = parse_time_trigger_string(line)
            self.assertEqual(processed_tuple, expected_output)

    def test_formatter(self):
        fixture = {RepeatTimer(300, None): 'every 300',
                   RepeatTimer(500, None): 'every 500',
                   RepeatTimer(1, None): 'every 1',
                   EventClock(['*-17:00', '4-15:45', '*-09:00'], None): 'at *-17:00,4-15:45,*-09:00',
                   EventClock(['5-18:00', '4-18:05', '1-9:01'], None): 'at 5-18:00,4-18:05,1-09:01',
                   EventClock(['*-08:01'], None): 'at *-08:01',
                   EventClock(['8:30'], None): 'at *-08:30'}

        for handler, expected_output in fixture.iteritems():
            processed_tuple = format_time_trigger_string(handler)
            self.assertEqual(processed_tuple, expected_output)

    def test_next_run_in(self):
        # 2014-05-01 is Thu. In Python it is weekday=3
        fixed_utc_now = \
            datetime(year=2014, month=05, day=01, hour=13, minute=00, second=00, microsecond=00, tzinfo=None)
        fixture = {EventClock(['*-17:00', '4-15:45', '*-09:00'], None):
                       timedelta(days=0, hours=4, minutes=0, seconds=0, microseconds=0, milliseconds=0),
                   EventClock(['5-18:00', '4-18:05', '1-9:01'], None):
                       timedelta(days=1, hours=5, minutes=5, seconds=0, microseconds=0, milliseconds=0),
                   EventClock(['*-08:01'], None):
                       timedelta(days=0, hours=19, minutes=1, seconds=0, microseconds=0, milliseconds=0),
                   EventClock(['8:30'], None):
                       timedelta(days=0, hours=19, minutes=30, seconds=0, microseconds=0, milliseconds=0)}

        for handler, expected_output in fixture.iteritems():
            handler.is_alive = lambda: True
            processed_output = handler.next_run_in(utc_now=fixed_utc_now)
            self.assertEqual(processed_output, expected_output)


if __name__ == '__main__':
    unittest.main()
