__author__ = 'Bohdan Mushkevych'

from synergy.conf import context
from synergy.conf import settings
from synergy.system.data_logging import Logger
from synergy.system.decorator import current_process_aware
from synergy.db.model.daemon_process_entry import DaemonProcessEntry


class ProcessContext(object):
    _current_process_name = ''

    # holds Logger instance per process name (and optional suffix)
    logger_pool = dict()

    # holds all registered processes, environment-aware
    CONTEXT = context.process_context

    def __init__(self):
        super(ProcessContext, self).__init__()

    @classmethod
    def put(cls, context_entry):
        assert isinstance(context_entry, DaemonProcessEntry)
        cls.CONTEXT[context_entry.process_name] = context_entry

    @classmethod
    @current_process_aware
    def get(cls, process_name=None):
        """
        :return instance of either [DaemonProcessEntry, FreerunProcessEntry, ManagedProcessEntry]
                associated with the process_name
        :raise KeyError if no entry is associated with given process_name
        """
        return cls.CONTEXT[process_name]

    @classmethod
    def set_current_process(cls, process_name):
        if cls._current_process_name:
            raise AttributeError('Current process %s is already set' % cls._current_process_name)
        cls._current_process_name = process_name

    @classmethod
    def get_current_process(cls):
        if not cls._current_process_name:
            raise AttributeError('Current process is not yet set')
        return cls._current_process_name


if __name__ == '__main__':
    pass
