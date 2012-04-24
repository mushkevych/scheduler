#!/usr/bin/env python
# -*- coding: utf-8 -*-
from os import path
from optparse import OptionParser
import shutil
import os
import sys
import time
import signal
import virtualenv
import subprocess

from system import process_context
from system.process_context import ProcessContext

PROCESS_STARTER = 'process_starter.py'
PROJECT_ROOT = path.abspath(path.dirname(__file__))

VE_SCRIPT = path.join(PROJECT_ROOT, 'scripts', 'install_virtualenv')
VE_ROOT = path.join(PROJECT_ROOT, '.ve')

# pylint: disable=C0301
PROCESSES_FOR_XXL = [process_context.PROCESS_STREAM_GEN, process_context.PROCESS_SCHEDULER,
                     process_context.PROCESS_SESSION_WORKER_00, process_context.PROCESS_SESSION_WORKER_01,
                     process_context.PROCESS_GC,
                     process_context.PROCESS_SITE_HOURLY, process_context.PROCESS_SITE_DAILY,
                     process_context.PROCESS_SITE_MONTHLY, process_context.PROCESS_SITE_YEARLY,
                     process_context.PROCESS_CLIENT_DAILY, process_context.PROCESS_CLIENT_MONTHLY,
                     process_context.PROCESS_CLIENT_YEARLY, process_context.PROCESS_ALERT_DAILY]
# pylint: enable=C0301

def init_parser():
    """Setup our command line options"""
    parser = OptionParser()
    parser.add_option("-a", "--app", action="store",
                      help="application to start (process name)")
    parser.add_option("-n", "--interactive", action="store_true",
                      help="run in interactive (non-daemon) mode")
    parser.add_option("-r", "--run", action="store_true",
                      help="start process supervisor for this box")
    parser.add_option("-k", "--kill", action="store_true",
                      help="kill process supervisor for this box")
    parser.add_option("-q", "--query", action="store_true",
                      help="query box configuration for current box")
    parser.add_option("-i", "--install_ve", action="store_true",
                      help="install a virtualenv for the runtime to use")
    parser.add_option("-s", "--shell", action="store_true",
                      help="run an ipython shell within the virtualenv")
    parser.add_option("-t", "--tests", action="store_true",
                      help="run tests")
    parser.add_option("-c", "--continuous", action="store_true",
                      help="run tests when ever there is a change; with -t")
    parser.add_option("-x", "--xunit", action="store_true",
                      help="run tests with coverage and xunit output for hudson")
    parser.add_option("-l", "--lint", action="store_true",
                      help="run pylint on project")
    parser.add_option("-8", "--pep8", action="store_true",
                      help="run pep8 on project")
    parser.add_option("-o", "--outfile", action="store",
                      help="save results from a report to a file")
    return parser


def get_python():
    """Determine the path to the virtualenv python"""
    if sys.platform == 'win32':
        python = path.join(VE_ROOT, 'Scripts', 'python.exe')
    else:
        python = path.join(VE_ROOT, 'bin', 'python')
    return python


def go_to_ve():
    """Rerun this script within the virtualenv with same args"""
    if not path.abspath(sys.prefix) == VE_ROOT:
        python = get_python()
        retcode = subprocess.call([python, __file__] + sys.argv[1:])
        sys.exit(retcode)


def install_environment(root):
    """Install our virtual environment; removing the old one if it exists"""
    print 'Installing virtualenv into %s' % root
    if path.exists(root):
        shutil.rmtree(root)
    virtualenv.logger = virtualenv.Logger(consumers=[])
    virtualenv.create_environment(root, site_packages=False)
    retcode = subprocess.call([VE_SCRIPT, PROJECT_ROOT, root])
    sys.exit(retcode)


def install_or_switch_to_virtualenv(options):
    """Installs, switches, or bails"""
    if options.install_ve:
        install_environment(VE_ROOT)
    elif path.exists(VE_ROOT):
        go_to_ve()
    else:
        sys.stdout.write('No virtualenv detected, please run ./launch.py --install_ve \n')
        sys.exit(1)


def dispatch_options(parser, options, args):
    if options.run:
        daemonize = True
        if options.interactive:
            daemonize = False
        start_process(options, daemonize)
    elif options.kill:
        stop_process(options)
    elif options.query:
        query_configuration(options)
    elif options.shell:
        run_shell(options)
    elif options.lint:
        run_lint(options)
    elif options.pep8:
        run_pep8(options)
    elif options.tests:
        run_tests(options)
    elif options.xunit:
        run_xunit(options)
    else:
        parser.print_help()


def query_configuration(options):
    """ reads current box configuration and prints it to the console """
    import logging
    from supervisor import supervisor_helper as helper

    box_id = helper.get_box_id(logging)
    sys.stdout.write('\nConfiguration for BOX_ID=%r:\n' % box_id)
    box_configuration = helper.retrieve_configuration(logging, box_id)
    process_list = box_configuration.get_process_list()
    i = 1
    for process in process_list:
        sys.stdout.write('%d\t%r:%r \n' % (i, process, process_list[process]))
        i += 1
    sys.stdout.write('\n')

def _get_supervisor_pid():
    """ check for supervisor's pid file and returns pid from there """
    try:
        pid_filename = ProcessContext.get_pid_filename(process_context.PROCESS_SUPERVISOR)
        pf = file(pid_filename, 'r')
        pid = int(pf.read().strip())
        pf.close()
    except IOError:
        pid = None
    return pid


def start_process(options, daemonize):
    """Start up supervisor and proper synergy-data daemons"""
    import logging
    import psutil
    from settings import settings
    from supervisor import supervisor_helper as helper
    from model.box_configuration_entry import BoxConfigurationEntry

    box_id = helper.get_box_id(logging)
    if options.app is not None and options.app != process_context.PROCESS_SUPERVISOR:
        # mark individual process for execution
        # real work is performed by Supervisor
        if options.app not in PROCESSES_FOR_XXL:
            sys.stdout.write('ERROR: requested process must be withing allowed list of: %r \n' % PROCESSES_FOR_XXL)
            sys.exit(1)

        box_configuration = helper.retrieve_configuration(logging, box_id)
        box_configuration.set_process_state(options.app, BoxConfigurationEntry.STATE_ON)
        helper.update_configuration(logging, box_configuration)
    else:
        # start Supervisor
        try:
            pid = _get_supervisor_pid()
            if pid is not None:
                if psutil.pid_exists(pid):
                    message = 'ERROR: Supervisor is already running with pid %r\n' % pid
                    sys.stderr.write(message)
                    sys.exit(1)

            p = psutil.Popen([get_python(), PROJECT_ROOT + '/' + PROCESS_STARTER, process_context.PROCESS_SUPERVISOR],
                               close_fds=True,
                               cwd=settings['process_cwd'],
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
            sys.stdout.write('Started %s with pid = %r with configuration for BOX_ID=%r \n' \
                            % (process_context.PROCESS_SUPERVISOR, p.pid, box_id))
        except Exception as e:
            sys.stderr.write('Exception on starting %s : %s \n' % (process_context.PROCESS_SUPERVISOR, str(e)))

def stop_process(options):
    """Stop the synergy-data daemons"""
    import logging
    from supervisor import supervisor_helper as helper
    from model.box_configuration_entry import BoxConfigurationEntry

    if options.app is not None and options.app != process_context.PROCESS_SUPERVISOR:
        # mark individual process for termination
        # real work is performed by Supervisor
        if options.app not in PROCESSES_FOR_XXL:
            sys.stdout.write('ERROR: requested process must be withing allowed list of: %r \n' % PROCESSES_FOR_XXL)
            sys.exit(1)

        box_id = helper.get_box_id(logging)
        box_configuration = helper.retrieve_configuration(logging, box_id)
        box_configuration.set_process_state(options.app, BoxConfigurationEntry.STATE_OFF)
        helper.update_configuration(logging, box_configuration)
    else:
        # stop Supervisor
        try:
            pid = _get_supervisor_pid()
            if pid is None:
                message = 'ERROR: Can not find Supervisor pidfile. Supervisor not running?\n'
                sys.stderr.write(message)
                sys.exit(1)

            # Try killing the daemon process
            sys.stdout.write('INFO: Killing %r \n' % process_context.PROCESS_SUPERVISOR)
            while 1:
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.1)
            ProcessContext.remove_pid_file(process_context.PROCESS_SUPERVISOR)
        except Exception as e:
            sys.stderr.write('Exception on killing %s : %s \n' % (process_context.PROCESS_SUPERVISOR, str(e)))
            sys.exit(0)


def run_shell(options):
    """Run IPython in the virtualenv"""
    import IPython
    # Stolen from django
    # Explicitly pass an empty list as arguments, because otherwise IPython
    # would use sys.argv from this script.
    shell = IPython.Shell.IPShell(argv=[])
    shell.mainloop()


def run_lint(options):
    from pylint import lint
    from pylint.reporters.text import ParseableTextReporter
    from settings import testable_modules as modules

    if options.outfile:
        output = open(options.outfile, 'w')
    else:
        output = sys.stdout

    config = "--rcfile=" + path.join(PROJECT_ROOT, 'pylint.rc')
    lint.Run([config] + modules,
             reporter=ParseableTextReporter(output=output), exit=False)


def run_pep8(options):
    import pep8
    from settings import testable_modules as modules

    # alas, pep8 can out go to stdout
    arglist = ['-r'] + modules
    pep8.process_options(arglist)
    for module in modules:
        pep8.input_dir(module, runner=pep8.input_file)


def load_all_tests():
    import unittest
    from settings import test_cases

    return unittest.defaultTestLoader.loadTestsFromNames(test_cases)


def run_tests(options):
    import unittest
    import logging
    import settings

    settings.enable_test_mode()

    argv = [sys.argv[0]] + args
    try:
        unittest.main(module=None, defaultTest='__main__.load_all_tests',
                      argv=argv)
    except SystemExit, e:
        if e.code == 0:
            logging.info('PASS')
        else:
            logging.error('FAIL')
            raise
        if not options.continuous:
            raise
    if options.continuous:
        sys.stdout.write('WARNING: continuous test run not implemented for synergy-data\n')


def run_xunit(options):
    import unittest
    import xmlrunner
    import settings

    settings.enable_test_mode()

    output = 'reports'
    if options.outfile:
        output = options.outfile

    argv = [sys.argv[0]] + args
    try:
        unittest.main(
            module=None,
            defaultTest='__main__.load_all_tests',
            argv=argv,
            testRunner=xmlrunner.XMLTestRunner(output=output))
    except SystemExit:
        pass


# Ensure we are running in a virtual environment
if __name__ == "__main__":
    parser = init_parser()
    (options, args) = parser.parse_args()
    install_or_switch_to_virtualenv(options)
    dispatch_options(parser, options, args)
