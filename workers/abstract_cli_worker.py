__author__ = 'Bohdan Mushkevych'

import time
from datetime import datetime
from psutil.error import TimeoutExpired
from synergy.db.model import unit_of_work

from synergy.workers.abstract_uow_aware_worker import AbstractUowAwareWorker


class AbstractCliWorker(AbstractUowAwareWorker):
    """ Module contains common logic for Command Line Callers.
    It executes shell command and updates unit_of_work base on command's return code """

    def __init__(self, process_name):
        super(AbstractCliWorker, self).__init__(process_name)
        self.cli_process = None

    def __del__(self):
        super(AbstractCliWorker, self).__del__()

    # **************** Process Supervisor Methods ************************
    def _start_process(self, start_timeperiod, end_timeperiod, arguments):
        raise NotImplementedError('method _start_process must be implemented by AbstractCliWorker child classes')

    def _poll_process(self):
        """ between death of a process and its actual termination lies poorly documented requirement -
            <purging process' io pipes and reading exit status>.
            this can be done either by os.wait() or process.wait()
            @return tuple (boolean: alive, int: return_code) """
        try:
            self.logger.warn(self.cli_process.stderr.read())
            self.logger.info(self.cli_process.stdout.read())
            return_code = self.cli_process.wait(timeout=0.01)
            if return_code is None:
                # process is already terminated
                self.logger.info('Process %s is terminated' % self.process_name)
            else:
                # process is terminated; possibly by OS
                self.logger.info('Process %s got terminated. Cleaning up' % self.process_name)
            self.cli_process = None
            return False, return_code
        except TimeoutExpired:
            # process is alive and OK
            return True, None
        except Exception:
            self.logger.error('Exception on polling: %s' % self.process_name, exc_info=True)
            return False, 999

    def _process_uow(self, uow):
        self._start_process(uow.start_timeperiod, uow.end_timeperiod, uow.arguments)
        code = None
        alive = True
        while alive:
            alive, code = self._poll_process()
            time.sleep(0.1)

        if code == 0:
            uow.number_of_processed_documents = self.performance_ticker.per_job
            uow.finished_at = datetime.utcnow()
            uow.state = unit_of_work.STATE_PROCESSED
            self.performance_ticker.finish_uow()
            self.logger.info('Command Line Command return code is %r' % code)
            self.uow_dao.update(uow)
        else:
            raise UserWarning('Command Line Command return code is not 0 but %r' % code)
