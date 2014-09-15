__author__ = 'Bohdan Mushkevych'

import functools


def managed_entry_request(method):
    """ wraps method with verification for is_managed_request_valid"""

    @functools.wraps(method)
    def _wrapper(self, *args, **kwargs):
        if not self.is_managed_request_valid:
            return dict()
        try:
            return method(self, *args, **kwargs)
        except Exception as e:
            self.logger.error('MX Exception: %s' % str(e), exc_info=True)

    return _wrapper


def freerun_entry_request(method):
    """ wraps method with verification for is_freerun_request_valid"""

    @functools.wraps(method)
    def _wrapper(self, *args, **kwargs):
        if not self.is_freerun_request_valid:
            return dict()
        try:
            return method(self, *args, **kwargs)
        except Exception as e:
            self.logger.error('MX Exception: %s' % str(e), exc_info=True)

    return _wrapper
