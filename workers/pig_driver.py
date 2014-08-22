__author__ = 'Bohdan Mushkevych'

import psutil
from subprocess import PIPE

from settings import settings
from system.process_context import ProcessContext
from workers.abstract_cli_worker import AbstractCliWorker


class PigDriver(AbstractCliWorker):
    """Python process that starts Pig processing job, supervises its execution and updated unit_of_work"""

    def __init__(self, process_name):
        super(PigDriver, self).__init__(process_name)

    def _start_process(self, start_timeperiod, end_timeperiod):
        try:
            input_file = ProcessContext.get_source(self.process_name)

            self.logger.info('start: %s {' % self.process_name)
            p = psutil.Popen([settings['bash_shell'],
                              settings['pig_command'],
                              '-f', '/home/bmushkevych/git/synergy-pig/script.pig',
                              '-p', 'input_file=' + input_file + '/' + start_timeperiod,
                              '-p', 'timeperiod=' + start_timeperiod],
                             close_fds=True,
                             cwd=settings['process_cwd'],
                             stdin=PIPE,
                             stdout=PIPE,
                             stderr=PIPE)
            self.cli_process = p
            self.logger.info('Started %s with pid = %r' % (self.process_name, p.pid))
        except Exception:
            self.logger.error('Exception on starting: %s' % self.process_name, exc_info=True)
        finally:
            self.logger.info('}')

