"""
Created on 2012-04-24

@author: Bohdan Mushkevych
"""
import functools
import time
from pymongo.errors import AutoReconnect


def thread_safe(method):
    """ wraps function with lock acquire/release cycle """

    @functools.wraps(method)
    def _locker(self, *args, **kwargs):
        try:
            self.lock.acquire()
            return method(self, *args, **kwargs)
        finally:
            self.lock.release()
    return _locker


def with_reconnect(func):
    """
    Handle when AutoReconnect is raised from pymongo. This is the standard error
    raised for everything from "host disconnected" to "couldn't connect to host"
    and more.

    The sleep handles the edge case when the state of a replica set changes, and
    the cursor raises AutoReconnect because the master may have changed. It can
    take some time for the replica set to stop raising this exception, and the
    small sleep and iteration count gives us a couple of seconds before we fail
    completely.
    """

    @functools.wraps(func)
    def _reconnector(*args, **kwargs):
        for _ in xrange(0, 20):
            try:
                return func(*args, **kwargs)
            except AutoReconnect:
                time.sleep(0.250)
        raise
    return _reconnector