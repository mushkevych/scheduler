__author__ = 'Bohdan Mushkevych'

import functools


def valid_action_request(method):
    """ wraps method with verification for is_request_valid"""

    @functools.wraps(method)
    def _wrapper(self, *args, **kwargs):
        if not self.is_request_valid:
            return dict()

        try:
            return method(self, *args, **kwargs)
        except UserWarning as e:
            self.logger.error('MX Processing Exception: %s' % str(e), exc_info=True)
            return dict()
        except Exception as e:
            self.logger.error('MX Exception: %s' % str(e), exc_info=True)
            return dict()

    return _wrapper
