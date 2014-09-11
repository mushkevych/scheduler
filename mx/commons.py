__author__ = 'Bohdan Mushkevych'

import functools


def valid_only(method):
    """ wraps method with verification for _valid_ and """

    @functools.wraps(method)
    def _wrapper(self, *args, **kwargs):
        if not self.valid:
            return dict()
        try:
            return method(self, *args, **kwargs)
        except Exception as e:
            self.logger.error('MX Exception: %s' % str(e), exc_info=True)

    return _wrapper
