__author__ = 'Bohdan Mushkevych'

from subprocess import PIPE

import psutil

from conf import settings
from conf.process_context import ProcessContext
from workers.abstract_cli_worker import AbstractCliWorker


class PigDriver(AbstractCliWorker):
    """Python process that starts Pig processing job, supervises its execution and updates unit_of_work"""

    def __init__(self, process_name):
        super(PigDriver, self).__init__(process_name)

    def _start_process(self, start_timeperiod, end_timeperiod, arguments):
        try:
            input_file = ProcessContext.get_source(self.process_name)

            self.logger.info('start: %s {' % self.process_name)
            p = psutil.Popen([settings.settings['bash_shell'],
                              settings.settings['pig_command'],
                              '-f', '/home/bmushkevych/git/synergy-pig/script.pig',
                              '-p', 'input_file=' + input_file + '/' + start_timeperiod,
                              '-p', 'timeperiod=' + start_timeperiod],
                             close_fds=True,
                             cwd=settings.settings['process_cwd'],
                             stdin=PIPE,
                             stdout=PIPE,
                             stderr=PIPE)
            self.cli_process = p
            self.logger.info('Started %s with pid = %r' % (self.process_name, p.pid))
        except Exception:
            self.logger.error('Exception on starting: %s' % self.process_name, exc_info=True)
        finally:
            self.logger.info('}')
