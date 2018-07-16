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
    def __init__(self, interval, call_back, daemonic=None, args=None, kwargs=None):
        if not kwargs: kwargs = {}
        if not args: args = []
        
        threading.Thread.__init__(self)
        # handle daemonic state as in Python3
        if daemonic is not None:
            self.daemon = daemonic
        else:
            self.daemon = threading.current_thread().daemon

        assert isinstance(interval, numbers.Number)
        # interval_current shows number of seconds in currently triggered <tick>
        self.interval_current = float(interval)
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
        """ stops the timer. call_back function is not called """
        self.event.clear()
        if self.__timer is not None:
            self.__timer.cancel()

    def trigger(self):
        """ calls the call_back function. interrupts the timer to start a new countdown """
        self.call_back(*self.args, **self.kwargs)
        if self.__timer is not None:
            self.__timer.cancel()

    def change_interval(self, value):
        """ :param value: <tick> interval in seconds
            current countdown is not interrupted
            new interval will be applied after the trigger execution """
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
