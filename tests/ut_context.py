__author__ = 'Bohdan Mushkevych'

from model.process_context_entry import _process_context_entry
from context import ROUTING_IRRELEVANT, EXCHANGE_UTILS
from system.time_qualifier import *
from system.process_context import ProcessContext

# User fields
PROCESS_CLASS_EXAMPLE = 'AbstractClassWorker'
PROCESS_SCRIPT_EXAMPLE = 'ScriptExampleWorker'

# process provides <process context> to unit testing: such as logger, queue, etc
PROCESS_UNIT_TEST = 'UnitTest'

_TOKEN_CLASS_EXAMPLE = 'class_example'
_TOKEN_SCRIPT_EXAMPLE = 'script_example'


def register_processes():
    """ Function should be called by #setting.enable_test_mode to register UT classes and functionality """
    process_entry = _process_context_entry(
        process_name=PROCESS_SCRIPT_EXAMPLE,
        classname='workers.example_script_worker.main',
        token=_TOKEN_SCRIPT_EXAMPLE,
        time_qualifier=QUALIFIER_REAL_TIME,
        exchange=EXCHANGE_UTILS)
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_CLASS_EXAMPLE,
        classname='workers.abstract_worker.AbstractWorker.start',
        token=_TOKEN_CLASS_EXAMPLE,
        time_qualifier=QUALIFIER_DAILY,
        exchange=EXCHANGE_UTILS)
    ProcessContext.put_process_entry(process_entry)

    process_entry = _process_context_entry(
        process_name=PROCESS_UNIT_TEST,
        classname='',
        token='unit_test',
        time_qualifier=QUALIFIER_REAL_TIME,
        routing=ROUTING_IRRELEVANT,
        exchange=EXCHANGE_UTILS)
    ProcessContext.put_process_entry(process_entry)
