__author__ = 'Bohdan Mushkevych'

import os
import psutil
from threading import Lock
from subprocess import PIPE, STDOUT
from psutil import TimeoutExpired

from launch import get_python, PROJECT_ROOT, PROCESS_STARTER
from db.model import box_configuration
from db.dao.box_configuration_dao import BoxConfigurationDao
from supervisor import supervisor_helper as helper
from system.process_context import ProcessContext
from system.repeat_timer import RepeatTimer
from system.synergy_process import SynergyProcess
from settings import settings

INTERVAL = 5    # seconds between checking process


class Supervisor(SynergyProcess):
    def __init__(self, process_name):
        super(Supervisor, self).__init__(process_name)
        self.pid = os.getpid()
        self.thread_handlers = dict()
        self.lock = Lock()
        self.box_id = helper.get_box_id(self.logger)
        self.bc_dao = BoxConfigurationDao(self.logger)
        self.logger.info('Started %s with configuration for BOX_ID=%r' % (self.process_name, self.box_id))

    def __del__(self):
        for handler in self.thread_handlers:
            handler.cancel()
        self.thread_handlers.clear()
        super(Supervisor, self).__del__()

    # **************** Supervisor Methods ************************
    def _kill_process(self, box_config, process_name):
        """ method is called to kill a running process """
        try:
            self.logger.info('kill: %s {' % process_name)
            pid = box_config.get_process_pid(process_name)
            if pid is not None and psutil.pid_exists(int(pid)):
                p = psutil.Process(pid)
                p.kill()
                p.wait()
                box_config.set_process_pid(process_name, None)
                self.bc_dao.update(box_config)
                ProcessContext.remove_pid_file(process_name)
        except Exception:
            self.logger.error('Exception on killing: %s' % process_name, exc_info=True)
        finally:
            self.logger.info('}')

    def _start_process(self, box_config, process_name):
        try:
            self.logger.info('start: %s {' % process_name)
            p = psutil.Popen([get_python(), PROJECT_ROOT + '/' + PROCESS_STARTER, process_name],
                             close_fds=True,
                             cwd=settings['process_cwd'],
                             stdin=PIPE,
                             stdout=PIPE,
                             stderr=STDOUT)
            box_config.set_process_pid(process_name, p.pid)
            self.logger.info('Started %s with pid = %r' % (process_name, p.pid))
        except Exception:
            box_config.set_process_pid(process_name, None)
            self.logger.error('Exception on starting: %s' % process_name, exc_info=True)
        finally:
            self.bc_dao.update(box_config)
            self.logger.info('}')

    def _poll_process(self, box_config, process_name):
        """ between killing a process and its actual termination lies poorly documented requirement -
            <purging process' io pipes and reading exit status>.
            this can be done either by os.wait() or process.wait() """
        try:
            pid = box_config.get_process_pid(process_name)
            p = psutil.Process(pid)

            return_code = p.wait(timeout=0.01)
            if return_code is None:
                # process is already terminated
                self.logger.info('Process %s is terminated' % process_name)
                return
            else:
                # process is terminated; possibly by OS
                box_config.set_process_pid(process_name, None)
                self.bc_dao.update(box_config)
                self.logger.info('Process %s got terminated. Cleaning up' % process_name)
        except TimeoutExpired:
            # process is alive and OK
            pass
        except Exception:
            self.logger.error('Exception on polling: %s' % process_name, exc_info=True)

    def start(self, *_):
        """ reading box configurations and starting timers to start/monitor/kill processes """
        try:
            box_config = self.bc_dao.get_one(self.box_id)
            process_list = box_config.process_list
            for process in process_list:
                params = [process]
                handler = RepeatTimer(INTERVAL, self.manage_process, args=params)
                self.thread_handlers[process] = handler
                handler.start()
                self.logger.info('Started Supervisor for %s, triggering every %d seconds' % (process, INTERVAL))
        except LookupError as e:
            self.logger.error('Supervisor failed to start because of: %r' % e)

    def manage_process(self, *args):
        """ reads box configuration and start/kill processes. performs process monitoring """
        process_name = args[0]
        try:
            self.lock.acquire()

            box_config = self.bc_dao.get_one(self.box_id)
            state = box_config.get_process_state(process_name)
            pid = box_config.get_process_pid(process_name)
            if state == box_configuration.STATE_OFF:
                if pid is not None:
                    self._kill_process(box_config, process_name)
                return

            if pid is None or not psutil.pid_exists(int(pid)):
                self._start_process(box_config, process_name)
            elif pid is not None and psutil.pid_exists(int(pid)):
                self._poll_process(box_config, process_name)
        except Exception as e:
            self.logger.error('Exception: %s' % str(e), exc_info=True)
        finally:
            self.lock.release()


if __name__ == '__main__':
    from system.process_context import PROCESS_SUPERVISOR

    source = Supervisor(PROCESS_SUPERVISOR)
    source.start()