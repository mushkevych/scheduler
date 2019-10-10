__author__ = 'Bohdan Mushkevych'

from threading import Thread
from werkzeug.wrappers import Request
from werkzeug.wsgi import ClosingIterator
from werkzeug.middleware.shared_data import SharedDataMiddleware
from werkzeug.exceptions import HTTPException, NotFound
from werkzeug.serving import run_simple
from synergy.conf import settings
from synergy.system.system_logger import get_logger
from synergy.scheduler.scheduler_constants import PROCESS_MX

from synergy.mx.utils import STATIC_PATH, local, local_manager, url_map, jinja_env
from synergy.mx import views
from flow.mx import views as flow_views
from flow.mx import STATIC_FLOW_ENDPOINT, STATIC_FLOW_PATH

import socket
socket.setdefaulttimeout(10.0)  # set default socket timeout at 10 seconds


class MX(object):
    """ MX stands for Management Extension and represents HTTP server serving UI front-end for Synergy Scheduler """
    def __init__(self, mbean):
        local.application = self
        self.mx_thread = None
        self.mbean = mbean
        jinja_env.globals['mbean'] = mbean

        self.dispatch = SharedDataMiddleware(self.dispatch, {
            '/static': STATIC_PATH,
            '/{0}'.format(STATIC_FLOW_ENDPOINT): STATIC_FLOW_PATH,
        })

        # during the get_logger call a 'werkzeug' logger will be created
        # later, werkzeug._internal.py -> _log() will assign the logger to global _logger variable
        self.logger = get_logger(PROCESS_MX)

    def dispatch(self, environ, start_response):
        local.application = self
        request = Request(environ)
        local.url_adapter = adapter = url_map.bind_to_environ(environ)
        local.request = request

        try:
            endpoint, values = adapter.match()

            # first - try to read from synergy.mx.views
            handler = getattr(views, endpoint, None)
            if not handler:
                # otherwise - read from flow.mx.views
                handler = getattr(flow_views, endpoint)

            response = handler(request, **values)
        except NotFound:
            response = views.not_found(request)
            response.status_code = 404
        except HTTPException as e:
            response = e
        return ClosingIterator(response(environ, start_response),
                               [local_manager.cleanup])

    def __call__(self, environ, start_response):
        return self.dispatch(environ, start_response)

    def start(self, hostname=None, port=None):
        """ Spawns a new HTTP server, residing on defined hostname and port
        :param hostname: the default hostname the server should listen on.
        :param port: the default port of the server.
        """
        if hostname is None:
            hostname = settings.settings['mx_host']
        if port is None:
            port = settings.settings['mx_port']

        reloader = False        # use_reloader: the default setting for the reloader.
        debugger = False        #
        evalex = True           # should the exception evaluation feature be enabled?
        threaded = False        # True if each request is handled in a separate thread
        processes = 1           # if greater than 1 then handle each request in a new process
        reloader_interval = 1   # the interval for the reloader in seconds.
        static_files = None     # static_files: optional dict of static files.
        extra_files = None      # extra_files: optional list of extra files to track for reloading.
        ssl_context = None      # ssl_context: optional SSL context for running server in HTTPS mode.

        self.mx_thread = Thread(target=run_simple(hostname=hostname,
                                                  port=port,
                                                  application=self,
                                                  use_debugger=debugger,
                                                  use_evalex=evalex,
                                                  extra_files=extra_files,
                                                  use_reloader=reloader,
                                                  reloader_interval=reloader_interval,
                                                  threaded=threaded,
                                                  processes=processes,
                                                  static_files=static_files,
                                                  ssl_context=ssl_context))
        self.mx_thread.daemon = True
        self.mx_thread.start()

    def stop(self):
        """ method stops currently running HTTP server, if any
            :see: `werkzeug.serving.make_environ`
            http://flask.pocoo.org/snippets/67/ """
        func = jinja_env.get('werkzeug.server.shutdown')
        if func is None:
            raise RuntimeError('MX Error: no Shutdown Function registered for the Werkzeug Server')
        func()


if __name__ == '__main__':
    from synergy.scheduler.scheduler_constants import PROCESS_SCHEDULER
    from synergy.scheduler.synergy_scheduler import Scheduler

    scheduler = Scheduler(PROCESS_SCHEDULER)
    app = MX(scheduler)
    app.start()
