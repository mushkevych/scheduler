__author__ = 'Bohdan Mushkevych'

import sys
import time
import functools
import traceback


def thread_safe(method):
    """ wraps function with lock acquire/release cycle
     decorator requires class instance to have field self.lock of type threading.Lock or threading.RLock """

    @functools.wraps(method)
    def _locker(self, *args, **kwargs):
        try:
            self.lock.acquire()
            return method(self, *args, **kwargs)
        finally:
            try:
                self.lock.release()
            except:
                sys.stderr.write('Exception on releasing lock at method {0}'.format(method.__name__))
                traceback.print_exc(file=sys.stderr)

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
    from pymongo.errors import AutoReconnect

    @functools.wraps(func)
    def _reconnector(*args, **kwargs):
        for _ in xrange(20):
            try:
                return func(*args, **kwargs)
            except AutoReconnect:
                time.sleep(0.250)
        raise

    return _reconnector


def singleton(cls):
    """
    turns class to singleton
    :param cls: class itself
    :return: function that either creates new instance of the class or returns existing one
    """

    # the only way to implement nonlocal closure variables in Python 2.X
    instances = {}

    def get_instance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]

    return get_instance
