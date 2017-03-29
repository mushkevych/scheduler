__author__ = 'Bohdan Mushkevych'

import json
from os import path
from datetime import datetime

from jinja2 import Environment, FileSystemLoader
from werkzeug.local import Local, LocalManager
from werkzeug.wrappers import Response
from werkzeug.routing import Map, Rule

from synergy.conf import context
from synergy.conf import settings

from flow.mx import STATIC_FLOW_ENDPOINT, STATIC_FLOW_PATH

TEMPLATE_PATH = path.join(path.dirname(__file__), 'templates')
STATIC_PATH = path.join(path.dirname(__file__), 'static')

local = Local()
local_manager = LocalManager([local])

# Synergy MX map of URL routing
url_map = Map()
url_map.add(Rule('/static/<file>', endpoint='static', build_only=True))
url_map.add(Rule('/{0}/<file>'.format(STATIC_FLOW_ENDPOINT), endpoint=STATIC_FLOW_ENDPOINT, build_only=True))

# tree/group of trees will be shown on a separate page defined by tree property MX_PAGE
# mx_page_context is a dictionary in format: {MX_PAGE: MX PAGE}
mx_page_context = {tree_entry.mx_page: tree_entry.mx_page.replace('_', ' ')
                   for tree_entry in context.timetable_context.values()}

# loop sets a Rule per every mx_page from mx_page_context to be processed by
# 'mx_page_tiles' method from mx.views.py
# NOTICE: given approach renders template snippet {{ url_for ('function_name') }} invalid,
# since all mx_page are processed by the single function 'mx_page_tiles'
for rule in mx_page_context:
    url_map.add(Rule('/{0}/'.format(rule), endpoint='mx_page_tiles'))


def expose(rule, methods=None, **kw):
    def decorate(f):
        url_map.add(Rule(rule, methods=methods, endpoint=f.__name__))
        return f
    return decorate


def url_for(endpoint, _external=False, **values):
    return local.url_adapter.build(endpoint, values, force_external=_external)


def render_template(template, **context):
    return Response(jinja_env.get_template(template).render(**context), mimetype='text/html')


def scheduler_uptime():
    time_diff = datetime.utcnow() - settings.settings['process_start_time']
    d = {'days': time_diff.days}
    d['hours'], rem = divmod(time_diff.seconds, 3600)
    d['minutes'], d['seconds'] = divmod(rem, 60)
    return '{days:02d}:{hours:02d}:{minutes:02d}:{seconds:02d}'.format(**d)


jinja_env = Environment(loader=FileSystemLoader([TEMPLATE_PATH, STATIC_FLOW_PATH]), autoescape=True)
jinja_env.add_extension('jinja2.ext.do')
jinja_env.globals['url_for'] = url_for
jinja_env.globals['local'] = local
jinja_env.globals['get_current_time'] = lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S %Z')
jinja_env.globals['scheduler_version'] = lambda: settings.settings['version']
jinja_env.globals['scheduler_uptime'] = scheduler_uptime
jinja_env.globals['mx_processing_context'] = mx_page_context
jinja_env.globals['synergy_process_context'] = context.process_context
jinja_env.filters['jsonify'] = json.dumps
jinja_env.filters['lstrip_slash'] = lambda x: x.lstrip('/')
