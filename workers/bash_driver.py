__author__ = 'Bohdan Mushkevych'

import os
import fabric.operations

from workers.abstract_mq_worker import AbstractMqWorker


ARGUMENT_SCRIPT_PATH = 'script_path'
ARGUMENT_SCRIPT_NAME = 'script_name'
ARGUMENT_SCRIPT_PARAMS = 'script_params'
ARGUMENT_HOST = 'host'


class BashDriver(AbstractMqWorker):
    """Process starts remote or local bash script job, supervises its execution and updates unit_of_work"""

    def __init__(self, process_name):
        super(BashDriver, self).__init__(process_name)
        self.is_alive = False
        self.return_code = -1

    def _poll_process(self):
        return self.is_alive, self.return_code

    def _start_process(self, arguments):
        try:
            self.is_alive = True
            self.logger.info('start: %s {' % self.process_name)
            fabric.operations.env.warn_only = True
            fabric.operations.env.abort_on_prompts = True
            fabric.operations.env.use_ssh_config = True
            fabric.operations.env.host_string = arguments[ARGUMENT_HOST]

            command = os.path.join(arguments[ARGUMENT_SCRIPT_PATH], arguments[ARGUMENT_SCRIPT_NAME])
            command += ' %s' % arguments.get(ARGUMENT_SCRIPT_PARAMS, '')

            run_result = fabric.operations.run(command)
            if run_result.succeeded:
                self.return_code = 0

            self.logger.info('Completed %s with result = %r' % (self.process_name, self.return_code))
        except Exception:
            self.logger.error('Exception on starting: %s' % self.process_name, exc_info=True)
        finally:
            self.logger.info('}')
            self.is_alive = False

    def _mq_callback(self, message):
        """ reads JSON request from the mq message and delivers it for processing """
        try:
            self._start_process(message.body)
            code = None
            alive = True
            while alive:
                alive, code = self._poll_process()

            if code == 0:
                self.performance_ticker.tracker.increment_success()
            else:
                self.performance_ticker.tracker.increment_failure()

            self.logger.info('BashDriver for %s return code is %r' % (message.body, code))
        except Exception as e:
            self.performance_ticker.tracker.increment_failure()
            self.logger.error('Safety fuse while processing request %r: %r' % (message.body, e), exc_info=True)
        finally:
            self.consumer.acknowledge(message.delivery_tag)
            self.consumer.close()


if __name__ == '__main__':
    from constants import PROCESS_BASH_DRIVER

    source = BashDriver(PROCESS_BASH_DRIVER)
    source.start()
