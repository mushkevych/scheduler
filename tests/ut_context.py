__author__ = 'Bohdan Mushkevych'

from context import ROUTING_IRRELEVANT, EXCHANGE_UTILS
from synergy.conf.process_context import ProcessContext
from synergy.db.model.daemon_process_entry import deamon_context_entry

# User fields
PROCESS_CLASS_EXAMPLE = 'AbstractClassWorker'
PROCESS_SCRIPT_EXAMPLE = 'ScriptExampleWorker'

# process provides <process context> to unit testing: such as logger, queue, etc
PROCESS_UNIT_TEST = 'UnitTest'

TOKEN_CLASS_EXAMPLE = 'class_example'
TOKEN_SCRIPT_EXAMPLE = 'script_example'
TOKEN_UNIT_TEST = 'unit_test'


def register_processes():
    """ Function should be called by #setting.enable_test_mode to register UT classes and functionality """
    process_entry = deamon_context_entry(
        process_name=PROCESS_SCRIPT_EXAMPLE,
        classname='workers.example_script_worker.main',
        token=TOKEN_SCRIPT_EXAMPLE,
        exchange=EXCHANGE_UTILS)
    ProcessContext.put(process_entry)

    process_entry = deamon_context_entry(
        process_name=PROCESS_CLASS_EXAMPLE,
        classname='synergy.workers.abstract_mq_worker.AbstractMqWorker.start',
        token=TOKEN_CLASS_EXAMPLE,
        exchange=EXCHANGE_UTILS)
    ProcessContext.put(process_entry)

    process_entry = deamon_context_entry(
        process_name=PROCESS_UNIT_TEST,
        classname='',
        token=TOKEN_UNIT_TEST,
        routing=ROUTING_IRRELEVANT,
        exchange=EXCHANGE_UTILS)
    ProcessContext.put(process_entry)
