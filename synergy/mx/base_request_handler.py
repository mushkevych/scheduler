__author__ = 'Bohdan Mushkevych'

import functools

from werkzeug.wrappers import Request
from synergy.mx.utils import jinja_env


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


class BaseRequestHandler(object):
    def __init__(self, request, **values):
        assert isinstance(request, Request)
        self.scheduler = jinja_env.globals['mbean']
        self.logger = self.scheduler.logger
        self.request = request
        self.values = values
        self.request_arguments = request.args if request.args else request.form
        self.is_request_valid = False

