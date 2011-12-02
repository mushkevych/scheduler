"""
Created on 2011-06-15

@author: Bohdan Mushkevych
"""

from system.process_context import ProcessContext
import sys

def get_class(kls):
    parts = kls.split('.')
    module = ".".join(parts[:-1])
    m = __import__( module )
    for comp in parts[1:]:
        m = getattr(m, comp)
    return m

def run(process_name):
    """
    You should override this method when you subclass Daemon. It will be
    called after the process has been daemonized by start() or restart().
    """

    sys.stdout.write('INFO: Starting %s \n' % ProcessContext.get_classname(process_name))
    klass = get_class(ProcessContext.get_classname(process_name))
    instance = klass(process_name)
    instance.start()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.stdout.write('ERROR: no Process Name specified to start')
    else:
        process_name = sys.argv[1]
        run(process_name)