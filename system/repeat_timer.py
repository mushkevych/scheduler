"""
Created on 2011-02-10

@author: Brian Curtin
http://code.activestate.com/lists/python-ideas/8982/
"""
from datetime import datetime

import threading

class RepeatTimer(threading.Thread):
    def __init__(self, interval, callable, args=[], kwargs={}):
        threading.Thread.__init__(self)
        # interval_current shows number of milliseconds in currently triggered <tick>
        self.interval_current = interval
        # interval_new shows number of milliseconds for next <tick>
        self.interval_new = interval
        self.callable = callable
        self.args = args
        self.kwargs = kwargs
        self.event = threading.Event()
        self.event.set()
        self.activation_dt = None

    def run(self):
        while self.event.is_set():
            self.activation_dt = datetime.utcnow()
            t = threading.Timer(self.interval_new,
                                self.callable,
                                self.args, 
                                self.kwargs)
            self.interval_current = self.interval_new
            t.start()
            t.join()

    def cancel(self):
        self.event.clear()

    def change_interval(self, value):
        self.interval_new = value
