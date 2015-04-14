__author__ = 'Bohdan Mushkevych'

import functools

from werkzeug.wrappers import Request
from synergy.mx.utils import jinja_env


def valid_action_request(method):
    """ wraps method with verification for is_request_valid"""

    @functools.wraps(method)
    def _wrapper(self, *args, **kwargs):
        assert isinstance(self, BaseRequestHandler)
        if not self.is_request_valid:
            return self.reply_bad_request()

        try:
            return method(self, *args, **kwargs)
        except UserWarning as e:
            return self.reply_server_error(e)
        except Exception as e:
            return self.reply_server_error(e)

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

    def reply_ok(self):
        return {'status': 'OK'}

    def reply_bad_request(self):
        self.logger.error('Bad request: {0}'.format(self.request))
        return {}

    def reply_server_error(self, e):
        self.logger.error('MX Processing Exception: {0}'.format(e), exc_info=True)
        return {'status': 'Server Internal Error'}
