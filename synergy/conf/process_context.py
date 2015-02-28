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

    @classmethod
    @current_process_aware
    def get_logger(cls, process_name=None):
        """ method returns initiated logger"""
        if process_name not in cls.logger_pool:
            file_name = cls.get_log_filename(process_name)
            tag = cls.get_log_tag(process_name)
            cls.logger_pool[process_name] = Logger(file_name, tag)
        return cls.logger_pool[process_name].get_logger()

    @classmethod
    @current_process_aware
    def get_classname(cls, process_name=None):
        """ method returns fully qualified classname of the instance running as process"""
        return cls.CONTEXT[process_name].classname

    @classmethod
    @current_process_aware
    def get_log_filename(cls, process_name=None):
        """method returns path for the Log filename"""
        return settings.settings['log_directory'] + cls.CONTEXT[process_name].log_filename

    @classmethod
    @current_process_aware
    def get_log_tag(cls, process_name=None):
        """method returns tag that all logging messages will be marked with"""
        return cls.CONTEXT[process_name].log_tag

    @classmethod
    @current_process_aware
    def get_time_qualifier(cls, process_name=None):
        """ method returns worker/aggregator time scale (like daily or yearly)"""
        return cls.CONTEXT[process_name].time_qualifier

    @classmethod
    @current_process_aware
    def get_sink(cls, process_name=None):
        """ method returns name of the data sink, as specified for the process"""
        return cls.CONTEXT[process_name].sink

    @classmethod
    @current_process_aware
    def get_source(cls, process_name=None):
        """ method returns name of the data source, as specified for the process"""
        return cls.CONTEXT[process_name].source

    @classmethod
    @current_process_aware
    def get_process_type(cls, process_name=None):
        """ method returns process type
        Supported types are listed in process_context starting with TYPE_ prefix
        and are defined in ProcessContextEntry """
        return cls.CONTEXT[process_name].process_type

    @classmethod
    @current_process_aware
    def get_blocking_type(cls, process_name=None):
        """ method returns process blocking type
        Supported types are listed in process_context starting with BLOCKING_ prefix
        and are defined in ProcessContextEntry """
        return cls.CONTEXT[process_name].process_type

    @classmethod
    @current_process_aware
    def get_arguments(cls, process_name=None):
        """ method returns process-specific arguments, defined during declaration\
         arguments are presented as key-value dictionary """
        return cls.CONTEXT[process_name].arguments

    @classmethod
    @current_process_aware
    def run_on_active_timeperiod(cls, process_name=None):
        """ method returns True if the process should wait for the timeperiod to complete before it starts
        and False if the process could start as soon as the timeperiod arrives """
        return cls.CONTEXT[process_name].run_on_active_timeperiod


if __name__ == '__main__':
    pass
