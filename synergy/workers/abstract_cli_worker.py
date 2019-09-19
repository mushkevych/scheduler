__author__ = 'Bohdan Mushkevych'

import time

from psutil import TimeoutExpired
from synergy.db.model import unit_of_work
from synergy.workers.abstract_uow_aware_worker import AbstractUowAwareWorker

RETURN_CODE_CANCEL_UOW = 99998888
RETURN_CODE_NOOP_UOW = 77776666


class AbstractCliWorker(AbstractUowAwareWorker):
    """ Module contains common logic for Command Line Callers.
        It executes shell command and updates unit_of_work base on command's return code """

    def __init__(self, process_name, perform_db_logging=False):
        super(AbstractCliWorker, self).__init__(process_name, perform_db_logging)
        self.cli_process = None

    def __del__(self):
        super(AbstractCliWorker, self).__del__()

    def _start_process(self, start_timeperiod, end_timeperiod, arguments):
        raise NotImplementedError(f'method _start_process must be implemented by {self.__class__.__name__}')

    def _poll_process(self):
        """ between death of a process and its actual termination lies poorly documented requirement -
            <purging process' io pipes and reading exit status>.
            this can be done either by os.wait() or process.wait()
            :return tuple (boolean: alive, int: return_code) """
        try:
            self.logger.warning(self.cli_process.stderr.read())
            self.logger.info(self.cli_process.stdout.read())
            return_code = self.cli_process.wait(timeout=0.01)
            if return_code is None:
                # process is already terminated
                self.logger.info(f'Process {self.process_name} is terminated')
            else:
                # process is terminated; possibly by OS
                self.logger.info(f'Process {self.process_name} got terminated. Cleaning up')
            self.cli_process = None
            return False, return_code
        except TimeoutExpired:
            # process is alive and OK
            return True, None
        except Exception:
            self.logger.error(f'Exception on polling: {self.process_name}', exc_info=True)
            return False, 999

    def _process_uow(self, uow):
        self._start_process(uow.start_timeperiod, uow.end_timeperiod, uow.arguments)
        code = None
        alive = True
        while alive:
            alive, code = self._poll_process()
            time.sleep(0.1)

        self.logger.info(f'Command Line Command return code is {code}')
        if code == 0:
            return 0, unit_of_work.STATE_PROCESSED
        elif code == RETURN_CODE_CANCEL_UOW:
            return 0, unit_of_work.STATE_CANCELED
        elif code == RETURN_CODE_NOOP_UOW:
            return 0, unit_of_work.STATE_NOOP
        else:
            raise UserWarning(f'Command Line Command return code is not 0 but {code}')
