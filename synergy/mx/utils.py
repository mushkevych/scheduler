__author__ = 'Bohdan Mushkevych'

import json
from os import path
from datetime import datetime
from urlparse import urlparse

from jinja2 import Environment, FileSystemLoader
from werkzeug.local import Local, LocalManager
from werkzeug.wrappers import Response
from werkzeug.routing import Map, Rule

from synergy.conf import context
from synergy.conf import settings

TEMPLATE_PATH = path.join(path.dirname(__file__), 'templates')
STATIC_PATH = path.join(path.dirname(__file__), 'static')
ALLOWED_SCHEMES = frozenset(['http', 'https', 'ftp', 'ftps'])
URL_CHARS = 'abcdefghijkmpqrstuvwxyzABCDEFGHIJKLMNPQRST23456789'

local = Local()
local_manager = LocalManager([local])

url_map = Map([Rule('/static/<file>', endpoint='static', build_only=True)])

# loop sets a Rule per every mx_page from context.mx_page_context to be processed by
# 'processing_details' method from mx.views.py
# NOTE: given renders template snippet {{ url_for ('function_name') }} invalid,
# since all mx_page are processed by single method 'processing_details'
for rule in context.mx_page_context:
    url_map.add(Rule('/%s/' % rule, endpoint='processing_details'))


def expose(rule, **kw):
    def decorate(f):
        url_map.add(Rule(rule, endpoint=f.__name__))
        return f

    return decorate


def url_for(endpoint, _external=False, **values):
    return local.url_adapter.build(endpoint, values, force_external=_external)


def render_template(template, **context):
    return Response(jinja_env.get_template(template).render(**context),
                    mimetype='text/html')


def validate_url(url):
    return urlparse(url)[0] in ALLOWED_SCHEMES


def get_current_time():
    return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S %Z')


def get_version():
    return settings.settings['version']


jinja_env = Environment(loader=FileSystemLoader(TEMPLATE_PATH))
jinja_env.globals['url_for'] = url_for
jinja_env.globals['local'] = local
jinja_env.globals['get_current_time'] = get_current_time
jinja_env.globals['get_version'] = get_version
jinja_env.globals['mx_processing_context'] = context.mx_page_context
jinja_env.filters['jsonify'] = json.dumps
