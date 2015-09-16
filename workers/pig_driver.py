__author__ = 'Bohdan Mushkevych'

from subprocess import PIPE

import psutil

from synergy.conf import settings
from synergy.conf import context
from synergy.workers.abstract_cli_worker import AbstractCliWorker


# to use following driver, make sure settings.py declares following properties:
# pig_command='/usr/bin/pig'
class PigDriver(AbstractCliWorker):
    """Python process that starts Pig processing job, supervises its execution and updates unit_of_work"""

    def __init__(self, process_name):
        super(PigDriver, self).__init__(process_name)

    def _start_process(self, start_timeperiod, end_timeperiod, arguments):
        try:
            input_file = context.process_context[self.process_name].source

            self.logger.info('start: {0} {{'.format(self.process_name))
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
            self.logger.info('Started {0} with pid = {1}'.format(self.process_name, p.pid))
        except Exception:
            self.logger.error('Exception on starting: {0}'.format(self.process_name), exc_info=True)
        finally:
            self.logger.info('}')
