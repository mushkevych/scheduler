__author__ = 'Bohdan Mushkevych'

from subprocess import PIPE

import psutil

from system import time_helper
from system.time_qualifier import QUALIFIER_HOURLY
from conf import settings
from conf.process_context import ProcessContext
from workers.abstract_cli_worker import AbstractCliWorker


class SqoopDriver(AbstractCliWorker):
    """Process starts Sqoop import job, supervises its execution and updates unit_of_work"""

    SQOOP_DATE_FORMAT = '%Y-%m-%d %H:%M:%S.000'

    def __init__(self, process_name):
        super(SqoopDriver, self).__init__(process_name)

    def _start_process(self, start_timeperiod, end_timeperiod, arguments):
        try:
            start_dt = time_helper.synergy_to_datetime(QUALIFIER_HOURLY, start_timeperiod)
            sqoop_slice_starttime = start_dt.strftime(SqoopDriver.SQOOP_DATE_FORMAT)

            end_dt = time_helper.synergy_to_datetime(QUALIFIER_HOURLY, end_timeperiod)
            sqoop_slice_endtime = end_dt.strftime(SqoopDriver.SQOOP_DATE_FORMAT)

            sink_path = ProcessContext.get_sink(self.process_name)

            self.logger.info('start: %s {' % self.process_name)
            p = psutil.Popen([settings.settings['bash_shell'],
                              settings.settings['sqoop_command'],
                              str(sqoop_slice_starttime),
                              str(sqoop_slice_endtime),
                              sink_path + '/' + start_timeperiod],
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
