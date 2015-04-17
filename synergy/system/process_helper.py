__author__ = 'Bohdan Mushkevych'

from subprocess import PIPE
import sys

import psutil
from psutil import TimeoutExpired

from launch import get_python, PROJECT_ROOT, PROCESS_STARTER
from synergy.system.utils import remove_pid_file, get_pid_filename
from synergy.conf import settings


def get_process_pid(process_name):
    """ check for process' pid file and returns pid from there """
    try:
        pid_filename = get_pid_filename(process_name)
        with open(pid_filename, mode='r') as pid_file:
            pid = int(pid_file.read().strip())
    except IOError:
        pid = None
    return pid


def kill_process(process_name):
    """ method is called to kill a running process """
    try:
        sys.stdout.write('killing: {0} {{ \n'.format(process_name))
        pid = get_process_pid(process_name)
        if pid is not None and psutil.pid_exists(int(pid)):
            p = psutil.Process(pid)
            p.kill()
            p.wait()
            remove_pid_file(process_name)
    except Exception as e:
        sys.stderr.write('Exception on killing {0} : {1} \n'.format(process_name, str(e)))
    finally:
        sys.stdout.write('}')


def start_process(process_name, *args):
    try:
        sys.stdout.write('starting: {0} {{ \n'.format(process_name))
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
                         stderr=PIPE)
        sys.stdout.write('Started {0} with pid = {1} \n'.format(process_name, p.pid))
    except Exception as e:
        sys.stderr.write('Exception on starting {0} : {1} \n'.format(process_name, str(e)))
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
            sys.stdout.write('PID file was not found. Process {0} is likely terminated.\n'.format(process_name))
            return False

        p = psutil.Process(pid)
        return_code = p.wait(timeout=0.01)

        if return_code is None:
            # process is already terminated
            sys.stdout.write('Process {0} is terminated \n'.format(process_name))
            return False
        else:
            # process is terminated; possibly by OS
            sys.stdout.write('Process {0} got terminated \n'.format(process_name))
            return False
    except TimeoutExpired:
        sys.stdout.write('Process {0} is alive and OK \n'.format(process_name))
        return True
