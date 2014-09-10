#!/usr/bin/env python
# -*- coding: utf-8 -*-
# framework is available at github: https://github.com/mushkevych/launch.py 

""" 
    @author Bohdan Mushkevych
    @author Shawn MacIntyre
"""
import shutil
import sys
import traceback
import subprocess
from optparse import OptionParser
from os import path

from system import process_starter


PROJECT_ROOT = path.abspath(path.dirname(__file__))

# script is run to install virtual environment library into the global interpreter
VE_GLOBAL_SCRIPT = path.join(PROJECT_ROOT, 'scripts', 'install_ve_globally.sh')

# script creates virtual environment for the project
VE_SCRIPT = path.join(PROJECT_ROOT, 'scripts', 'install_virtualenv.sh')
VE_ROOT = path.join(PROJECT_ROOT, '.ve')


def init_parser():
    """Setup our command line options"""
    parser = OptionParser()
    parser.add_option("-n", "--interactive", action="store_true", help="run in interactive (non-daemon) mode")
    parser.add_option("-r", "--run", action="store_true", help="starts process identified by -app parameter")
    parser.add_option("-k", "--kill", action="store_true", help="kill process identified by -app parameter")
    parser.add_option("-a", "--app", action="store", help="application to start (process name)")
    parser.add_option("-q", "--query", action="store_true", help="query application's state")
    parser.add_option("-i", "--install_ve", action="store_true", help="install a virtualenv for the runtime to use")
    parser.add_option("-s", "--shell", action="store_true", help="run an ipython shell within the virtualenv")
    parser.add_option("-t", "--tests", action="store_true", help="run tests")
    parser.add_option("-x", "--xunit", action="store_true", help="run tests with coverage and xunit output for Jenkins")
    parser.add_option("-z", "--analyze", action="store_true", help="run pylint on project")
    parser.add_option("-l", "--list", action="store_true", help="list available applications")
    parser.add_option("-v", "--supervisor", action="store_true", help="run within supervisor")
    parser.add_option("-o", "--outfile", action="store", help="save results from a report to a file")
    return parser


def get_python():
    """Determine the path to the virtualenv python"""
    if sys.platform == 'win32':
        python = path.join(VE_ROOT, 'Scripts', 'python.exe')
    else:
        python = path.join(VE_ROOT, 'bin', 'python')
    return python


def go_to_ve():
    """Rerun this script within the virtualenv with same args
    Note: parent process will wait for created subprocess to complete"""
    # two options are possible
    if not path.abspath(sys.prefix) == VE_ROOT:
        # Option A: we are in the parental process that was called from command line like
        # $> ./launch.py --run -app NAME
        # in this case sys.prefix points to Global Interpreter
        python = get_python()
        retcode = subprocess.call([python, __file__] + sys.argv[1:])
        sys.exit(retcode)
    else:
        # Option B: we have already followed Option A and instantiated Virtual Environment command
        # This mean that sys.prefix points to Virtual Environment
        pass


def install_environment(root):
    """Install our virtual environment; removing the old one if it exists"""
    sys.stdout.write('Installing virtualenv into %s \n' % root)
    try:
        import virtualenv
    except ImportError:
        sys.stdout.write('Installing virtualenv into global interpreter \n')
        subprocess.call([VE_GLOBAL_SCRIPT, PROJECT_ROOT])
        import virtualenv

    if path.exists(root):
        shutil.rmtree(root)
    virtualenv.logger = virtualenv.Logger(consumers=[])
    virtualenv.create_environment(root, site_packages=False)
    ret_code = subprocess.call([VE_SCRIPT, PROJECT_ROOT, root])
    sys.exit(ret_code)


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
        start_process(options, args)
    elif options.kill:
        stop_process(options)
    elif options.query:
        query_configuration(options)
    elif options.shell:
        run_shell(options)
    elif options.analyze:
        run_lint(options)
    elif options.list:
        list_processes(options)
    elif options.tests:
        run_tests(options)
    elif options.xunit:
        run_xunit(options)
    else:
        parser.print_help()


def valid_process_name(function):
    """ Decorator validates if the --app parameter is registered in the process_context
        :raise #ValueError otherwise """
    from system.process_context import ProcessContext

    def _wrapper(options, *args, **kwargs):
        if options.app not in ProcessContext.CONTEXT:
            msg = 'Aborting: application <%r> defined by --app option is unknown. \n' % options.app
            sys.stdout.write(msg)
            raise ValueError(msg)
        return function(options, *args, **kwargs)

    return _wrapper


@valid_process_name
def query_configuration(options):
    """ Queries process state """
    from system import process_helper

    if not options.supervisor:
        # reads status of one process only
        process_helper.poll_process(options.app)
    else:
        # reads current box configuration and prints it to the console
        from db.dao.box_configuration_dao import BoxConfigurationDao
        from supervisor import supervisor_helper as helper
        from system.process_context import ProcessContext
        from constants import PROCESS_LAUNCH_PY

        logger = ProcessContext.get_logger(PROCESS_LAUNCH_PY)
        box_id = helper.get_box_id(logger)
        bc_dao = BoxConfigurationDao(logger)
        sys.stdout.write('\nConfiguration for BOX_ID=%r:\n' % box_id)
        box_configuration = bc_dao.get_one(box_id)
        process_list = box_configuration.get_process_list()
        i = 1
        for process in process_list:
            sys.stdout.write('%d\t%r:%r \n' % (i, process, process_list[process]))
            i += 1
        sys.stdout.write('\n')


@valid_process_name
def start_process(options, args):
    """Start up specific daemon """
    import psutil
    from system import process_helper
    from supervisor import supervisor_helper as helper
    from system.process_context import ProcessContext
    from supervisor.constants import PROCESS_SUPERVISOR
    from constants import PROCESS_LAUNCH_PY

    logger = ProcessContext.get_logger(PROCESS_LAUNCH_PY)
    box_id = helper.get_box_id(logger)
    if options.supervisor is True and options.app != PROCESS_SUPERVISOR:
        from db.model import box_configuration
        from db.dao.box_configuration_dao import BoxConfigurationDao

        message = 'INFO: Marking %r to be managed by Supervisor \n' % options.app
        sys.stdout.write(message)

        bc_dao = BoxConfigurationDao(logger)
        box_config = bc_dao.get_one(box_id)
        box_config.set_process_state(options.app, box_configuration.STATE_ON)
        bc_dao.update(box_config)
        return

    try:
        pid = process_helper.get_process_pid(options.app)
        if pid is not None:
            if psutil.pid_exists(pid):
                message = 'ERROR: Process %r is already running with pid %r\n' % (options.app, pid)
                sys.stderr.write(message)
                sys.exit(1)

        if not options.interactive:
            # this block triggers if the options.interactive is not defined or is False
            process_helper.start_process(options.app, args)
        else:
            process_starter.start_by_process_name(options.app, args)
    except Exception as e:
        sys.stderr.write('Exception on starting %s : %s \n' % (options.app, str(e)))
        traceback.print_exc(file=sys.stderr)


@valid_process_name
def stop_process(options):
    """Stop specific daemon"""
    from system import process_helper
    from supervisor import supervisor_helper as helper
    from system.process_context import ProcessContext
    from supervisor.constants import PROCESS_SUPERVISOR
    from constants import PROCESS_LAUNCH_PY

    logger = ProcessContext.get_logger(PROCESS_LAUNCH_PY)
    box_id = helper.get_box_id(logger)
    if options.supervisor is True and options.app != PROCESS_SUPERVISOR:
        from db.model import box_configuration
        from db.dao.box_configuration_dao import BoxConfigurationDao

        message = 'INFO: Marking %r to be managed by Supervisor \n' % options.app
        sys.stdout.write(message)

        bc_dao = BoxConfigurationDao(logger)
        box_config = bc_dao.get_one(box_id)
        box_config.set_process_state(options.app, box_configuration.STATE_OFF)
        bc_dao.update(box_config)
        return
    try:
        pid = process_helper.get_process_pid(options.app)
        if pid is None or process_helper.poll_process(options.app) is False:
            message = 'ERROR: Process %r is already terminated %r\n' % (options.app, pid)
            sys.stderr.write(message)
            sys.exit(1)

        process_helper.kill_process(options.app)
    except Exception as e:
        sys.stderr.write('Exception on killing %s : %s \n' % (options.app, str(e)))
        traceback.print_exc(file=sys.stderr)


def run_shell(options):
    """Run IPython in the virtualenv
    http://ipython.org/ipython-doc/stable/interactive/reference.html
    """
    from IPython import embed
    embed()


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


def list_processes(options):
    from system.process_context import ProcessContext
    msg = 'List of registered applications: %r \n' % ProcessContext.CONTEXT.keys()
    sys.stdout.write(msg)


def load_all_tests():
    import unittest
    from settings import test_cases

    return unittest.defaultTestLoader.loadTestsFromNames(test_cases)


def run_tests(options):
    import unittest
    import settings

    settings.enable_test_mode()
    argv = [sys.argv[0]] + args
    try:
        unittest.main(module=None, defaultTest='__main__.load_all_tests',
                      argv=argv)
    except SystemExit as e:
        from system.process_context import ProcessContext
        from constants import PROCESS_LAUNCH_PY

        logger = ProcessContext.get_logger(PROCESS_LAUNCH_PY)
        if e.code == 0:
            logger.info('PASS')
        else:
            logger.error('FAIL')
            raise


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
