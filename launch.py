#!/usr/bin/env python
# -*- coding: utf-8 -*-
# framework is available at github: https://github.com/mushkevych/launch.py 

""" 
    @author Bohdan Mushkevych
"""
import sys
import traceback
import subprocess
import argparse

from os import path


PROCESS_STARTER = 'process_starter.py'
PROJECT_ROOT = path.abspath(path.dirname(__file__))

# script creates virtual environment for the project
VE_SCRIPT = path.join(PROJECT_ROOT, 'scripts', 'install_virtualenv.sh')
VE_ROOT = path.join(PROJECT_ROOT, '.ve')


def init_parser():
    try:
        from synergy.conf import context, settings
        process_names = list(context.process_context)
        config_file = settings.settings['config_file']
    except ImportError:
        process_names = ['Virtual Environment is pending installation']
        config_file = 'Virtual Environment is pending installation'

    main_parser = argparse.ArgumentParser(prog='launch.py')
    subparsers = main_parser.add_subparsers(title='sub-commands', description='list of available sub-commands')

    install_parser = subparsers.add_parser('install', help='install a virtualenv for the runtime to use')
    install_parser.set_defaults(func=install_virtualenv)

    shell_parser = subparsers.add_parser('shell', help='run an iPython shell within the virtualenv')
    shell_parser.set_defaults(func=run_shell)

    list_parser = subparsers.add_parser('list', help='list available processes')
    list_parser.set_defaults(func=list_processes)

    db_parser = subparsers.add_parser('db', help='manages Synergy Scheduler DB')
    db_parser.set_defaults(func=db_command)
    db_group = db_parser.add_mutually_exclusive_group()
    db_group.add_argument('--update', action='store_true',
                          help='updates managed_process table with context.process_context records')
    db_group.add_argument('--reset', action='store_true',
                          help='drops the *scheduler* database, resets schema, updates managed_process table')

    super_parser = subparsers.add_parser('super', help='super a process by name')
    super_parser.set_defaults(func=supervisor_command)
    super_group = super_parser.add_mutually_exclusive_group()
    super_group.add_argument('--start', action='store_true', help='marks the process to be started by Supervisor')
    super_group.add_argument('--stop', action='store_true', help='marks the process to be stopped by Supervisor')
    super_group.add_argument('--query', action='store_true', help='queries the state of Supervisor-managed processes')
    super_group.add_argument('--update', action='store_true',
                             help='updates synergy.box_configuration table '
                                  'with context.process_context records applicable to this BOX_ID only')
    super_group.add_argument('--reset', action='store_true',
                             help='defines Supervisor DB schema. recreates synergy.box_configuration table')
    super_group.add_argument('--boxid', action='store_true',
                             help=f'creates {config_file} and records BOX_ID in it')
    super_parser.add_argument('argument', nargs='?')

    start_parser = subparsers.add_parser('start', help='start a process by name')
    start_parser.set_defaults(func=start_process)
    start_parser.add_argument('process_name', choices=process_names)
    start_parser.add_argument('--console', action='store_true', help='process is run in interactive (non-daemon) mode')

    stop_parser = subparsers.add_parser('stop', help='kill a process by name')
    stop_parser.set_defaults(func=stop_process)
    stop_parser.add_argument('process_name', choices=process_names)

    query_parser = subparsers.add_parser('query',
                                         help='query a process state [RUNNING, TERMINATED] by name')
    query_parser.set_defaults(func=query_configuration)
    query_parser.add_argument('process_name', nargs='?', choices=process_names)

    test_parser = subparsers.add_parser('test', help='run unit tests from the settings.test_cases list')
    test_parser.add_argument('-o', '--outfile', action='store', help='save report results into a file')
    test_parser.set_defaults(func=run_tests)
    test_group = test_parser.add_mutually_exclusive_group()
    test_group.add_argument('-x', '--xunit', action='store_true',
                            help='measure coverage during unit tests execution')
    test_group.add_argument('-p', '--pylint', action='store_true', help='run pylint on the project')

    return main_parser


def get_python():
    """Determine the path to the virtualenv python"""
    if sys.platform == 'win32':
        python = path.join(VE_ROOT, 'Scripts', 'python.exe')
    else:
        python = path.join(VE_ROOT, 'bin', 'python')
    return python


def go_to_ve():
    """ Rerun this script within the virtualenv with same args
        NOTICE: parent process will wait for created subprocess to complete """
    # two options are possible
    if not path.abspath(sys.prefix) == VE_ROOT:
        # Option A: we are in the parental process that was called from command line like
        # $> ./launch.py start PROCESS_NAME
        # in this case sys.prefix points to Global Interpreter
        python = get_python()
        retcode = subprocess.call([python, __file__] + sys.argv[1:])
        sys.exit(retcode)
    else:
        # Option B: we have already followed Option A and instantiated Virtual Environment command
        # This mean that sys.prefix points to Virtual Environment
        pass


def install_virtualenv(parser_args):
    """ Installs virtual environment """
    python_version = '.'.join(str(v) for v in sys.version_info[:2])
    sys.stdout.write(f'Installing Python {python_version} virtualenv into {VE_ROOT} \n')
    if sys.version_info < (3, 7):
        raise NotImplementedError('Scheduler requires Python 3.7+')

    # Install virtual environment for Python 3.7+; removing the old one if it exists
    import venv
    builder = venv.EnvBuilder(system_site_packages=False, clear=True, symlinks=False, upgrade=False, with_pip=True)
    builder.create(VE_ROOT)
    ret_code = subprocess.call([VE_SCRIPT, PROJECT_ROOT, VE_ROOT, python_version])
    sys.exit(ret_code)


def query_configuration(parser_args):
    """ Queries process state """
    from synergy.system import process_helper
    from synergy.conf import context

    process_names = [parser_args.process_name] if parser_args.process_name else list(context.process_context)
    for process_name in process_names:
        process_helper.poll_process(process_name)
    sys.stdout.write('\n')


def db_command(parser_args):
    """ Manages Synergy DB state """
    from synergy.db.manager import db_manager

    if parser_args.reset:
        db_manager.reset_db()
        # initialize DB with the context.process_context entries
        parser_args.update = True

    if parser_args.update:
        db_manager.update_db()


def supervisor_command(parser_args):
    """ Supervisor-related commands """
    import logging
    from synergy.supervisor.supervisor_configurator import SupervisorConfigurator, set_box_id

    if parser_args.boxid:
        set_box_id(logging, parser_args.argument)
        return

    sc = SupervisorConfigurator()
    if parser_args.reset:
        sc.reset_db()

    if parser_args.update:
        sc.update_db()

    elif parser_args.start:
        sc.mark_for_start(parser_args.argument)

    elif parser_args.stop:
        sc.mark_for_stop(parser_args.argument)

    elif parser_args.query:
        sc.query()


def start_process(parser_args):
    """ Start up specific daemon """
    import psutil
    import process_starter
    import settings
    from synergy.system import process_helper, utils

    utils.ensure_dir(settings.settings['pid_directory'])
    utils.ensure_dir(settings.settings['log_directory'])

    try:
        pid = process_helper.get_process_pid(parser_args.process_name)
        if pid is not None:
            if psutil.pid_exists(pid):
                message = f'ERROR: Process {parser_args.process_name} is already running with pid {pid}\n'
                sys.stderr.write(message)
                sys.exit(1)

        if not parser_args.console:
            # this block triggers if the options.console is not defined or is False
            process_helper.start_process(parser_args.process_name, parser_args.extra_parameters)
        else:
            process_starter.start_by_process_name(parser_args.process_name, parser_args.extra_parameters)
    except Exception as e:
        sys.stderr.write(f'Exception on starting {parser_args.process_name} : {e}\n')
        traceback.print_exc(file=sys.stderr)


def stop_process(parser_args):
    """ Stop/Kill specific daemon"""
    from synergy.system import process_helper

    try:
        pid = process_helper.get_process_pid(parser_args.process_name)
        if pid is None or process_helper.poll_process(parser_args.process_name) is False:
            message = f'ERROR: Process {parser_args.process_name} is already terminated {pid}\n'
            sys.stderr.write(message)
            sys.exit(1)

        process_helper.kill_process(parser_args.process_name)
    except Exception as e:
        sys.stderr.write(f'Exception on killing {parser_args.process_name} : {e}\n')
        traceback.print_exc(file=sys.stderr)


def run_shell(parser_args):
    """ Run IPython in the virtualenv
        http://ipython.org/ipython-doc/stable/interactive/reference.html """
    from IPython import embed

    embed()


def list_processes(parser_args):
    from synergy.conf import context

    process_names = list(context.process_context)
    msg = f'List of registered processes: {process_names} \n'
    sys.stdout.write(msg)


def load_all_tests():
    import unittest
    from settings import test_cases

    return unittest.defaultTestLoader.loadTestsFromNames(test_cases)


def run_tests(parser_args):
    import unittest
    import logging
    import settings
    settings.enable_test_mode()

    def unittest_main(test_runner=None):
        try:
            argv = [sys.argv[0]]
            if parser_args.extra_parameters:
                argv += parser_args.extra_parameters
            else:
                # workaround to avoid full unit test discovery
                # and limit test suite to settings.test_cases
                # argv.append('__main__.load_all_tests')
                argv.append('__main__.load_all_tests')

            unittest.main(module=None, argv=argv, testRunner=test_runner)
        except SystemExit as e:
            if e.code == 0:
                logging.info('PASS')
            else:
                logging.error('FAIL')
                raise

    if parser_args.pylint:
        from pylint import lint
        from pylint.reporters.text import ParseableTextReporter

        output = sys.stdout
        if parser_args.outfile:
            output = open(parser_args.outfile, 'w')

        config = "--rcfile=" + path.join(PROJECT_ROOT, 'pylint.rc')
        lint.Run([config] + settings.testable_modules,
                 reporter=ParseableTextReporter(output=output), exit=False)

    elif parser_args.xunit:
        import xmlrunner

        output = 'reports'
        if parser_args.outfile:
            output = parser_args.outfile
        unittest_main(xmlrunner.XMLTestRunner(output=output))

    else:
        unittest_main(None)


if __name__ == '__main__':
    python = get_python()
    if path.exists(python):
        go_to_ve()

    parser = init_parser()
    parser_namespace, extra_parameters = parser.parse_known_args()
    parser_namespace.extra_parameters = extra_parameters

    if parser_namespace.func != install_virtualenv:
        # before calling any sub-commands, switch to virtual environment
        if path.exists(VE_ROOT):
            go_to_ve()
        else:
            sys.stdout.write('No virtual environment detected. Run ./launch.py install \n')
            sys.exit(1)

    # calling a function associated with the sub-command in *parser.set_defaults(func=...)*
    parser_namespace.func(parser_namespace)
