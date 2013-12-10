__author__ = 'Bohdan Mushkevych'

import psutil
from subprocess import PIPE
from settings import settings
from system import time_helper
from system.process_context import PROCESS_SITE_HOURLY, ProcessContext
from workers.abstract_cli_worker import AbstractCliWorker


class SqoopDriver(AbstractCliWorker):
    """Process starts Sqoop import job, supervises its execution and updated unit_of_work"""

    SQOOP_DATE_FORMAT = '%Y-%m-%d %H:%M:%S.000'

    def __init__(self, process_name):
        super(SqoopDriver, self).__init__(process_name)

    def _start_process(self, start_timeperiod, end_timeperiod):
        try:
            start_dt = time_helper.synergy_to_datetime(ProcessContext.QUALIFIER_HOURLY, start_timeperiod)
            sqoop_slice_starttime = start_dt.strftime(SqoopDriver.SQOOP_DATE_FORMAT)

            end_dt = time_helper.synergy_to_datetime(ProcessContext.QUALIFIER_HOURLY, end_timeperiod)
            sqoop_slice_endtime = end_dt.strftime(SqoopDriver.SQOOP_DATE_FORMAT)

            sink_path = ProcessContext.get_target_collection(self.process_name)

            self.logger.info('start: %s {' % self.process_name)
            p = psutil.Popen([settings['bash_shell'],
                              settings['sqoop_command'],
                              str(sqoop_slice_starttime),
                              str(sqoop_slice_endtime),
                              sink_path + '/' + start_timeperiod],
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
