from synergy.system import repeat_timer

__author__ = 'Bohdan Mushkevych'

import unittest
from datetime import datetime
import time


class TestRepeatTimer(unittest.TestCase):
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
        raise AssertionError('Assertion failed as NO was executed with parameters %s %s' % (str(start_datetime), seconds))

    def method_checkpoint(self, start_datetime, seconds):
        print 'Entering method checkpoint with parameters %s %s' % (str(start_datetime), seconds)
        delta = datetime.utcnow() - start_datetime
        if delta.seconds != seconds:
            raise AssertionError('Assertion failed by %s % s' % (str(delta), seconds))

    def test_normal_workflow(self):
        self.obj = repeat_timer.RepeatTimer(TestRepeatTimer.INTERVAL,
                                            self.method_checkpoint,
                                            args=[datetime.utcnow(), TestRepeatTimer.INTERVAL])
        self.obj.start()
        time.sleep(TestRepeatTimer.INTERVAL)
        self.obj.cancel()
        assert True

    def test_cancellation(self):
        self.obj = repeat_timer.RepeatTimer(TestRepeatTimer.INTERVAL,
                                            self.method_no,
                                            args=[datetime.utcnow(), 0])
        self.obj.start()
        self.obj.cancel()
        time.sleep(TestRepeatTimer.INTERVAL)
        assert True

    def test_trigger(self):
        self.obj = repeat_timer.RepeatTimer(TestRepeatTimer.INTERVAL,
                                            self.make_method_yes(),
                                            args=[datetime.utcnow(), 0])
        self.obj.start()
        self.obj.trigger()
        self.obj.cancel()
        time.sleep(TestRepeatTimer.INTERVAL)
        assert True

    def test_trigger_with_continuation(self):
        self.obj = repeat_timer.RepeatTimer(TestRepeatTimer.INTERVAL - 1,
                                            self.make_method_yes(initial_multiplication=0),
                                            args=[datetime.utcnow(), TestRepeatTimer.INTERVAL - 1])
        self.obj.start()
        self.obj.trigger()
        time.sleep(TestRepeatTimer.INTERVAL)
        self.obj.cancel()
        assert True


if __name__ == '__main__':
    unittest.main()
