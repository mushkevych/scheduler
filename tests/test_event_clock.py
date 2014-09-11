__author__ = 'Bohdan Mushkevych'

import unittest
from datetime import datetime
import time

from system.event_clock import EventClock, EventTime


class TestEventClock(unittest.TestCase):
    INTERVAL = 3

    def make_method_yes(self, initial_multiplication=1):
        # the only way to implement nonlocal closure variables in Python 2.X
        cycle = {'index': initial_multiplication}

        def method_yes(start_datetime, seconds):
            delta = datetime.utcnow() - start_datetime
            assert delta.seconds == cycle['index'] * seconds
            cycle['index'] += 1

        return method_yes

    def method_no(self, start_datetime, seconds):
        raise AssertionError(
            'Assertion failed as NO was executed with parameters %s %s' % (str(start_datetime), seconds))

    def method_checkpoint(self, start_datetime, seconds):
        print 'Entering method checkpoint with parameters %s %s' % (str(start_datetime), seconds)
        delta = datetime.utcnow() - start_datetime
        if delta.seconds != seconds:
            raise AssertionError('Assertion failed by %s % s' % (str(delta), seconds))

    def test_utc_now(self):
        utc_now = datetime.utcnow()
        self.obj = EventTime.utc_now()
        print str(self.obj)
        assert self.obj.day_of_week == str(utc_now.weekday()) \
               and self.obj.time_of_day.hour == utc_now.hour \
               and self.obj.time_of_day.minute == utc_now.minute \
               and self.obj.time_of_day.second == 0 \
               and self.obj.time_of_day.microsecond == 0

    def test_eq(self):
        params = [EventTime(x) for x in ['17:00', '4-15:45', '*-09:00', '8:01']]
        expected = [EventTime(x) for x in ['*-17:00', '2-17:00', '4-15:45', '*-09:00', '*-9:00', '*-08:01']]
        not_expected = [EventTime(x) for x in ['*-17:15', '1-15:45', '*-9:01', '*-18:01']]

        self.assertEqual(EventTime('17:00'), EventTime('2-17:00'))
        self.assertEqual(EventTime('17:00'), EventTime('*-17:00'))

        for event in expected:
            self.assertIn(event, params)

        for event in not_expected:
            self.assertNotIn(event, params)


if __name__ == '__main__':
    unittest.main()
