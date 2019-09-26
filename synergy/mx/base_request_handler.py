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


def safe_json_response(method):
    """ makes sure the response' document has all leaf-fields converted to string """

    def _safe_document(document):
        """ function modifies the document in place
            it iterates over the json document and stringify all non-string types
            :return: modified document """
        assert isinstance(document, dict), 'Error: provided document is not of DICT type: {0}' \
            .format(document.__class__.__name__)

        for key, value in document.items():
            if isinstance(value, dict):
                document[key] = {k: str(v) for k, v in value.items()}
            elif isinstance(value, list):
                document[key] = [str(v) for v in value]
            else:
                document[key] = str(document[key])
        return document

    @functools.wraps(method)
    def _wrapper(self, *args, **kwargs):
        try:
            document = method(self, *args, **kwargs)
            return _safe_document(document)
        except Exception as e:
            return self.reply_server_error(e)

    return _wrapper


class BaseRequestHandler(object):
    def __init__(self, request: Request, **values):
        self.scheduler = jinja_env.globals['mbean']
        self.logger = self.scheduler.logger
        self.request = request
        self.values = values
        self.request_arguments = request.args if request.args else request.form
        self.is_request_valid = False

    def reply_ok(self):
        return {'status': 'OK'}

    def reply_bad_request(self):
        self.logger.error(f'MX Bad Request: {self.request}')
        return {}

    def reply_server_error(self, e):
        self.logger.error(f'MX Processing Exception: {e}', exc_info=True)
        return {'status': 'Server Internal Error'}
