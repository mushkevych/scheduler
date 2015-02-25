"""
@author: Bohdan Mushkevych
@author: Brian Curtin
http://code.activestate.com/lists/python-ideas/8982/
"""
import numbers
import threading
from datetime import datetime, timedelta


class RepeatTimer(threading.Thread):
    """ This class triggers every number of seconds """
    def __init__(self, interval, call_back, args=None, kwargs=None):
        if not kwargs: kwargs = {}
        if not args: args = []
        
        threading.Thread.__init__(self)
        assert isinstance(interval, numbers.Number)
        # interval_current shows number of seconds in currently triggered <tick>
        self.interval_current = interval
        # interval_new shows number of seconds for next <tick>
        self.interval_new = interval
        self.call_back = call_back
        self.args = args
        self.kwargs = kwargs
        self.event = threading.Event()
        self.event.set()
        self.activation_dt = None
        self.__timer = None

    def run(self):
        while self.event.is_set():
            self.activation_dt = datetime.utcnow()
            self.__timer = threading.Timer(self.interval_new,
                                           self.call_back,
                                           self.args,
                                           self.kwargs)
            self.interval_current = self.interval_new
            self.__timer.start()
            self.__timer.join()

    def cancel(self):
        self.event.clear()
        if self.__timer is not None:
            self.__timer.cancel()

    def trigger(self):
        self.call_back(*self.args, **self.kwargs)
        if self.__timer is not None:
            self.__timer.cancel()

    def change_interval(self, value):
        self.interval_new = value

    def next_run_in(self, utc_now=None):
        """ :param utc_now: optional parameter to be used by Unit Tests as a definition of "now"
            :return: timedelta instance presenting amount of time before the trigger is triggered next time
         or None if the RepeatTimer instance is not running """
        if utc_now is None:
            utc_now = datetime.utcnow()

        if self.is_alive():
            next_run = timedelta(seconds=self.interval_current) + self.activation_dt
            return next_run - utc_now
        else:
            return None
