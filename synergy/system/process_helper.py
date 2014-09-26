__author__ = 'Bohdan Mushkevych'

from subprocess import PIPE, STDOUT
import sys

import psutil
from psutil import TimeoutExpired

from launch import get_python, PROJECT_ROOT, PROCESS_STARTER
from synergy.conf.process_context import ProcessContext
from synergy.conf import settings


def get_process_pid(process_name):
    """ check for process' pid file and returns pid from there """
    try:
        pid_filename = ProcessContext.get_pid_filename(process_name)
        pf = file(pid_filename, 'r')
        pid = int(pf.read().strip())
        pf.close()
    except IOError:
        pid = None
    return pid


def kill_process(process_name):
    """ method is called to kill a running process """
    try:
        sys.stdout.write('killing: %s { \n' % process_name)
        pid = get_process_pid(process_name)
        if pid is not None and psutil.pid_exists(int(pid)):
            p = psutil.Process(pid)
            p.kill()
            p.wait()
            ProcessContext.remove_pid_file(process_name)
    except Exception as e:
        sys.stderr.write('Exception on killing %s : %s \n' % (process_name, str(e)))
    finally:
        sys.stdout.write('}')


def start_process(process_name, *args):
    try:
        sys.stdout.write('starting: %s { \n' % process_name)
        cmd = [get_python(), PROJECT_ROOT + '/' + PROCESS_STARTER, process_name]
        if not args:
            # this blocks triggers when args is either None or an empty list
            pass
        else:
            cmd.extend(*args)

        p = psutil.Popen(cmd,
                         close_fds=True,
                         cwd=settings.settings['process_cwd'],
                         stdin=PIPE,
                         stdout=PIPE,
                         stderr=STDOUT)
        sys.stdout.write('Started %s with pid = %r \n' % (process_name, p.pid))
    except Exception as e:
        sys.stderr.write('Exception on starting %s : %s \n' % (process_name, str(e)))
    finally:
        sys.stdout.write('}')


def poll_process(process_name):
    """ between killing a process and its actual termination lies poorly documented requirement -
        <purging process' io pipes and reading exit status>.
        this can be done either by os.wait() or process.wait()
        :return True if the process is alive and OK and False is the process was terminated """
    try:
        pid = get_process_pid(process_name)
        if pid is None:
            sys.stdout.write('PID file was not found. Process %s is likely terminated.\n' % process_name)
            return False

        p = psutil.Process(pid)
        return_code = p.wait(timeout=0.01)

        if return_code is None:
            # process is already terminated
            sys.stdout.write('Process %s is terminated \n' % process_name)
            return False
        else:
            # process is terminated; possibly by OS
            sys.stdout.write('Process %s got terminated \n' % process_name)
            return False
    except TimeoutExpired:
        sys.stdout.write('Process %s is alive and OK \n' % process_name)
        return True
