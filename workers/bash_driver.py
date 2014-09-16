__author__ = 'Bohdan Mushkevych'

import os
import fabric.operations

from workers.abstract_cli_worker import AbstractCliWorker


ARGUMENT_SCRIPT_PATH = 'script_path'
ARGUMENT_SCRIPT_NAME = 'script_name'
ARGUMENT_HOST = 'host'


class BashDriver(AbstractCliWorker):
    """Process starts remote or local bash script job, supervises its execution and updates unit_of_work"""

    def __init__(self, process_name):
        super(BashDriver, self).__init__(process_name)
        self.is_alive = False
        self.return_code = -1

    def _poll_process(self):
        return self.is_alive, self.return_code

    def _start_process(self, start_timeperiod, end_timeperiod, arguments):
        try:
            self.is_alive = True
            self.logger.info('start: %s {' % self.process_name)
            fabric.operations.env.warn_only = True
            fabric.operations.env.abort_on_prompts = True
            fabric.operations.env.use_ssh_config = True
            fabric.operations.env.host_string = arguments[ARGUMENT_HOST]

            command = os.path.join(arguments[ARGUMENT_SCRIPT_PATH], arguments[ARGUMENT_SCRIPT_NAME])
            command += ' %s %s' % (str(start_timeperiod), str(end_timeperiod))

            run_result = fabric.operations.run(command)
            if run_result.succeeded:
                self.return_code = 0

            self.logger.info('Completed %s with result = %r' % (self.process_name, self.return_code))
        except Exception:
            self.logger.error('Exception on starting: %s' % self.process_name, exc_info=True)
        finally:
            self.logger.info('}')
            self.is_alive = False
