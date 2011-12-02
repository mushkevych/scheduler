"""
Created on 2011-04-20

@author: Bohdan Mushkevych
"""

from datetime import datetime
from os import path
from urlparse import urlparse
from jinja2 import Environment, FileSystemLoader
from werkzeug.local import Local, LocalManager
from werkzeug.wrappers import Response
from werkzeug.routing import Map, Rule

TEMPLATE_PATH = path.join(path.dirname(__file__), 'templates')
STATIC_PATH = path.join(path.dirname(__file__), 'static')
ALLOWED_SCHEMES = frozenset(['http', 'https', 'ftp', 'ftps'])
URL_CHARS = 'abcdefghijkmpqrstuvwxyzABCDEFGHIJKLMNPQRST23456789'

local = Local()
local_manager = LocalManager([local])

url_map = Map([Rule('/static/<file>', endpoint='static', build_only=True)])

def expose(rule, **kw):
    def decorate(f):
        kw['endpoint'] = f.__name__
        url_map.add(Rule(rule, **kw))
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

def get_current_time():
    return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S %Z')


jinja_env = Environment(loader=FileSystemLoader(TEMPLATE_PATH))
jinja_env.globals['url_for'] = url_for
jinja_env.globals['local'] = local
jinja_env.globals['get_current_time'] = get_current_time
