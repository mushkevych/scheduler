__author__ = 'Bohdan Mushkevych'

import os

from synergy.conf import context
from synergy.conf import settings
from synergy.system.data_logging import Logger
from synergy.system.decorator import current_process_aware
from synergy.db.model.process_context_entry import ProcessContextEntry


class ProcessContext(object):
    _current_process_name = ''

    # holds Logger instance per process name (and optional suffix)
    logger_pool = dict()

    # holds all registered processes, environment-aware
    CONTEXT = context.process_context

    def __init__(self):
        super(ProcessContext, self).__init__()

    @classmethod
    def put_context_entry(cls, context_entry):
        assert isinstance(context_entry, ProcessContextEntry)
        cls.CONTEXT[context_entry.process_name] = context_entry

    @classmethod
    @current_process_aware
    def get_context_entry(cls, process_name=None):
        """ method returns dictionary of strings, preset
        source collection, target collection, queue name, exchange, routing, etc"""
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
    def create_pid_file(cls, process_name=None):
        """ creates pid file and writes os.pid() in there """
        pid_filename = cls.get_pid_filename(process_name)
        try:
            pid_file = open(pid_filename, mode='w')
            pid_file.write(str(os.getpid()))
        except Exception as e:
            cls.get_logger(process_name).error('Unable to create pid file at: %s, because of: %r' % (pid_filename, e))

    @classmethod
    @current_process_aware
    def remove_pid_file(cls, process_name=None):
        """ removes pid file """
        pid_filename = cls.get_pid_filename(process_name)
        try:
            os.remove(pid_filename)
            cls.get_logger(process_name).info('Removed pid file at: %s' % pid_filename)
        except Exception as e:
            cls.get_logger(process_name).error('Unable to remove pid file at: %s, because of: %r' % (pid_filename, e))

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
    def get_pid_filename(cls, process_name=None):
        """method returns path for the PID FILENAME """
        return settings.settings['pid_directory'] + cls.CONTEXT[process_name].pid_filename

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
    def get_routing(cls, process_name=None):
        """ method returns routing; it is used to segregate traffic within the queue
        for instance: routing_hourly for hourly reports, while routing_yearly for yearly reports"""
        return cls.CONTEXT[process_name].mq_routing_key

    @classmethod
    @current_process_aware
    def get_exchange(cls, process_name=None):
        """ method returns exchange for this classname.
        Exchange is a component that sits between queue and the publisher"""
        return cls.CONTEXT[process_name].mq_exchange

    @classmethod
    @current_process_aware
    def get_queue(cls, process_name=None):
        """ method returns queue that is applicable for the worker/aggregator, specified by classname"""
        return cls.CONTEXT[process_name].mq_queue

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
        Supported types are listed in process_context starting with TYPE_ prefix and are enumerated in
        scheduler.start() method"""
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
