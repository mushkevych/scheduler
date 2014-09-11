__author__ = 'Bohdan Mushkevych'

import unittest
from datetime import datetime
import time

from system.event_clock import EventClock, EventTime


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


if __name__ == '__main__':
    unittest.main()
