__author__ = 'Bohdan Mushkevych'

from datetime import time, datetime
from system.repeat_timer import RepeatTimer

TRIGGER_INTERVAL = 60  # 1 minute


class EventClock(object):
    """ This class triggers on predefined time set in HH:MM
    Maintaining API compatibility with the RepeatTimer class """
    def __init__(self, interval, call_back, args=[], kwargs={}):
        self.timestamps = []
        self.change_interval(interval)

        self.args = args
        self.kwargs = kwargs
        self.call_back = call_back
        self.handler = RepeatTimer(TRIGGER_INTERVAL, self.manage_schedule)

    def manage_schedule(self, *_):
        utc_now = datetime.utcnow()
        current_time = time(utc_now.hour, utc_now.minute)

        if current_time in self.timestamps:
            self.call_back(*self.args, **self.kwargs)

    def start(self):
        self.handler.start()

    def cancel(self):
        self.handler.cancel()

    def trigger(self):
        utc_now = datetime.utcnow()
        current_time = time(utc_now.hour, utc_now.minute)

        if current_time not in self.timestamps:
            self.call_back(*self.args, **self.kwargs)
        else:
            # leave it to the regular flow to trigger the call_back via manage_schedule method
            pass

    def change_interval(self, value):
        """ :param value: list of strings in format 'HH:MM' """
        assert not isinstance(value, str)
        self.timestamps = []

        for timestamp in self.timestamps:
            parsed_time = time.strptime(timestamp, "%H%M")
            self.timestamps.append(parsed_time)
