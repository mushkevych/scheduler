__author__ = 'Bohdan Mushkevych'

from subprocess import PIPE

import psutil
from synergy.conf import settings
from workers.abstract_cli_worker import AbstractCliWorker


class HadoopAggregatorDriver(AbstractCliWorker):
    """Python process that starts Hadoop map/reduce job, supervises its execution and updates unit_of_work"""

    def __init__(self, process_name):
        super(HadoopAggregatorDriver, self).__init__(process_name)

    def _start_process(self, start_timeperiod, end_timeperiod, arguments):
        try:
            self.logger.info('start: %s {' % self.process_name)
            p = psutil.Popen([settings.settings['hadoop_command'],
                              'jar', settings.settings['hadoop_jar'],
                              '-D', 'process.name=' + self.process_name,
                              '-D', 'timeperiod.working=' + str(start_timeperiod),
                              '-D', 'timeperiod.next=' + str(end_timeperiod)],
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
