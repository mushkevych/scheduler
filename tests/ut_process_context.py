__author__ = 'Bohdan Mushkevych'

from system.process_context import ProcessContext, _process_context_entry

# User fields
PROCESS_CLASS_EXAMPLE = 'AbstractClassWorker'
PROCESS_SCRIPT_EXAMPLE = 'ScriptExampleWorker'

# process provides <process context> to unit testing: such as logger, queue, etc
PROCESS_UNIT_TEST = 'UnitTest'

_TOKEN_CLASS_EXAMPLE = 'class_example'
_TOKEN_SCRIPT_EXAMPLE = 'script_example'


def register_unit_test_context():
    """ Function should be called by #setting.enable_test_mode to register UT classes and functionality """
    ProcessContext.PROCESS_CONTEXT[PROCESS_SCRIPT_EXAMPLE] = _process_context_entry(
        process_name=PROCESS_SCRIPT_EXAMPLE,
        classname='workers.example_script_worker.main',
        token=_TOKEN_SCRIPT_EXAMPLE,
        time_qualifier=ProcessContext.QUALIFIER_REAL_TIME,
        exchange=ProcessContext.EXCHANGE_UTILS)

    ProcessContext.PROCESS_CONTEXT[PROCESS_CLASS_EXAMPLE] = _process_context_entry(
        process_name=PROCESS_CLASS_EXAMPLE,
        classname='workers.abstract_worker.AbstractWorker.start',
        token=_TOKEN_CLASS_EXAMPLE,
        time_qualifier=ProcessContext.QUALIFIER_DAILY,
        exchange=ProcessContext.EXCHANGE_UTILS)

    ProcessContext.PROCESS_CONTEXT[PROCESS_UNIT_TEST] = _process_context_entry(
        process_name=PROCESS_UNIT_TEST,
        classname='',
        token='unit_test',
        time_qualifier=ProcessContext.QUALIFIER_REAL_TIME,
        routing=ProcessContext.ROUTING_IRRELEVANT,
        exchange=ProcessContext.EXCHANGE_UTILS)
