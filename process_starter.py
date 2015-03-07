__author__ = 'Bohdan Mushkevych'

import sys
import types
from six import class_types

from synergy.conf import context


def get_class(kls):
    """
    :param kls - string of fully identified starter function or starter method path
     for instance:
     - workers.abstract_worker.AbstractWorker.start
     - workers.example_script_worker.main
    :return tuple (type, object, starter)
     for instance:
     - (FunctionType, <function_main>, None)
     - (type, <Class_...>, 'start')
    """
    parts = kls.split('.')
    try:
        # First, try to import module hosting starter function
        module = '.'.join(parts[:-1])
        m = __import__(module)
    except ImportError:
        # If first try fails, try to import module hosting Class with a starter method
        module = '.'.join(parts[:-2])
        m = __import__(module)

    t = None
    starter = None
    for i in range(1, len(parts)):
        comp = parts[i]
        starter = parts[i:]
        m = getattr(m, comp)

        if isinstance(m, class_types):
            t = type
            starter = None if len(parts[i:]) == 1 else '.'.join(parts[i + 1:])
            break
        if isinstance(m, types.FunctionType):
            t = types.FunctionType
            starter = None
            break

    return t, m, starter


def start_by_process_name(process_name, *args):
    """
    Function starts the process by:
    1. retrieving its fully specified path name
    2. if the path name ends with starter method - then creates an instance of the wrapping class
        and calls <code>starter(*args)</code> method on it
    3. if the path name ends with starter function - then retrieves its module
        and calls <code>starter(*args)</code> function on it
    """
    sys.stdout.write('INFO: Starter path {0} \n'.format(context.process_context[process_name].classname))
    t, m, starter = get_class(context.process_context[process_name].classname)
    if isinstance(m, class_types):
        sys.stdout.write('INFO: Starting process by calling starter method {0} \n'.format(starter))
        instance = m(process_name)
        method = getattr(instance, starter)
        method(*args)
    elif isinstance(m, types.FunctionType):
        sys.stdout.write('INFO: Starting module.\n')
        function = m
        function(*args)
    else:
        raise ValueError('Improper starter path {0}'.format(context.process_context[process_name].classname))


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.stderr.write('ERROR: no Process Name specified to start \n')
    elif len(sys.argv) == 2:
        process_name = sys.argv[1]
        start_by_process_name(process_name, None)
    else:
        process_name = sys.argv[1]
        args = sys.argv[2:]
        start_by_process_name(process_name, args)
