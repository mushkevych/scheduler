__author__ = 'Bohdan Mushkevych'

import os
import time
import threading
import fabric.operations

from synergy.conf import settings
from synergy.workers.abstract_mq_worker import AbstractMqWorker
from synergy.db.model.worker_mq_request import WorkerMqRequest

ARGUMENT_SCRIPT_PATH = 'script_path'
ARGUMENT_SCRIPT_NAME = 'script_name'
ARGUMENT_SCRIPT_PARAMS = 'script_params'
ARGUMENT_HOST = 'host'


class BashRunnable(threading.Thread):
    """Process starts remote or local bash script job, supervises its execution and updates mq"""

    def __init__(self, logger, message, consumer, performance_ticker):
        self.logger = logger
        self.message = message
        self.mq_request = WorkerMqRequest(message.body)
        self.consumer = consumer
        self.performance_ticker = performance_ticker
        self.alive = False
        self.return_code = -1

        self.thread_name = '%s::%s' % (self.mq_request.process_name, self.mq_request.entry_name)
        super(BashRunnable, self).__init__(name=self.thread_name)

    def _poll_process(self):
        return self.alive, self.return_code

    def _start_process(self):
        try:
            self.logger.info('start: %s {' % self.thread_name)
            self.alive = True
            fabric.operations.env.warn_only = True
            fabric.operations.env.abort_on_prompts = True
            fabric.operations.env.use_ssh_config = True
            fabric.operations.env.host_string = self.mq_request.entry_arguments[ARGUMENT_HOST]

            command = os.path.join(self.mq_request.entry_arguments[ARGUMENT_SCRIPT_PATH],
                                   self.mq_request.entry_arguments[ARGUMENT_SCRIPT_NAME])
            command += ' %s' % self.mq_request.entry_arguments[ARGUMENT_SCRIPT_PARAMS]

            run_result = fabric.operations.run(command)
            if run_result.succeeded:
                self.return_code = 0

            self.logger.info('Completed %s with result = %r' % (self.thread_name, self.return_code))
        except Exception:
            self.logger.error('Exception on starting: %s' % self.thread_name, exc_info=True)
        finally:
            self.logger.info('}')
            self.is_alive = False

    def run(self):
        try:
            self._start_process()
            code = None
            alive = True
            while alive:
                alive, code = self._poll_process()

            if code == 0:
                self.performance_ticker.tracker.increment_success()
            else:
                self.performance_ticker.tracker.increment_failure()

            self.logger.info('BashDriver for %s return code is %r' % (self.thread_name, code))
        except Exception as e:
            self.performance_ticker.tracker.increment_failure()
            self.logger.error('Safety fuse while processing request %r: %r' % (self.message.body, e), exc_info=True)
        finally:
            self.consumer.acknowledge(self.message.delivery_tag)


class BashDriver(AbstractMqWorker):
    """Process facilitates threads running local or remote bash scripts"""

    def __init__(self, process_name):
        super(BashDriver, self).__init__(process_name)
        self.is_alive = False
        self.initial_thread_count = threading.active_count()

    def _mq_callback(self, message):
        """ reads JSON request from the mq message and delivers it for processing """
        while threading.active_count() > settings.settings['bash_runnable_count'] + self.initial_thread_count:
            time.sleep(0.01)

        t = BashRunnable(self.logger, message, self.consumer, self.performance_ticker)
        t.daemon = True
        t.start()


if __name__ == '__main__':
    from constants import PROCESS_BASH_DRIVER

    source = BashDriver(PROCESS_BASH_DRIVER)
    source.start()
