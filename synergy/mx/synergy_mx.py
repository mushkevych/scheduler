__author__ = 'Bohdan Mushkevych'

from threading import Thread
from werkzeug.wrappers import Request
from werkzeug.wsgi import ClosingIterator, SharedDataMiddleware
from werkzeug.exceptions import HTTPException, NotFound
from werkzeug.serving import run_simple
from synergy.conf import settings

from synergy.mx.utils import STATIC_PATH, local, local_manager, url_map, jinja_env
from synergy.mx import views


class MX(object):
    def __init__(self, mbean):
        local.application = self
        self.mx_thread = None
        self.mbean = mbean
        jinja_env.globals['mbean'] = mbean

        self.dispatch = SharedDataMiddleware(self.dispatch, {
            '/static': STATIC_PATH
        })

    def dispatch(self, environ, start_response):
        local.application = self
        request = Request(environ)
        local.url_adapter = adapter = url_map.bind_to_environ(environ)

        try:
            endpoint, values = adapter.match()
            handler = getattr(views, endpoint)
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

    def start_mx_thread(self, hostname=None, port=None):
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
        evalex = True           # use_evalex: the default setting for the evalex flag of the debugger.
        threaded = False        # threaded: the default threading setting.
        processes = 1           # processes: the default number of processes to start.
        reloader_interval = 1
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


if __name__ == '__main__':
    from synergy.scheduler.scheduler_constants import PROCESS_SCHEDULER
    from synergy.scheduler.synergy_scheduler import Scheduler

    source = Scheduler(PROCESS_SCHEDULER)

    app = MX(source)
    app.start_mx_thread()