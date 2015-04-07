__author__ = 'Bohdan Mushkevych'

import sys
import logging
import logging.handlers

from synergy.conf import settings
from synergy.conf import context
from synergy.db.model.daemon_process_entry import DaemonProcessEntry
from synergy.db.model.freerun_process_entry import FreerunProcessEntry
from synergy.db.model.managed_process_entry import ManagedProcessEntry


class Logger(object):
    """
    Logger presents standard API to log messages and store them for future analysis
    """

    def __init__(self, file_name, log_tag, append_to_console):
        """
        Constructor: dictionary of loggers available for this Python process
        :param file_name: path+name of the output file
        :param log_tag: tag that is printed ahead of every logged message
        :param append_to_console: True if messages should be printed to the terminal console
        """
        self.logger = logging.getLogger(log_tag)

        if append_to_console:
            # ATTENTION: while running as stand-alone process, stdout and stderr must be muted and redirected to file
            # otherwise the their pipes get overfilled, and process halts
            stream_handler = logging.StreamHandler()
            stream_formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
            stream_handler.setFormatter(stream_formatter)
            self.logger.addHandler(stream_handler)
        else:
            # While under_test, tools as xml_unittest_runner are doing complex sys.stdXXX reassignments
            sys.stdout = self
            sys.stderr = self

        if settings.settings['debug']:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        # ROTO FILE HANDLER:
        roto_file_handler = logging.handlers.RotatingFileHandler(file_name, maxBytes=2097152, backupCount=10)
        roto_file_formatter = logging.Formatter(fmt='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                                                datefmt='%Y-%m-%d %H:%M:%S')
        roto_file_handler.setFormatter(roto_file_formatter)
        self.logger.addHandler(roto_file_handler)

    def get_logger(self):
        return self.logger

    def write(self, msg, level=logging.INFO):
        """ method implements stream write interface, allowing to redirect stdout to logger """
        if msg is not None and len(msg.strip()) > 0:
            self.logger.log(level, msg)

    def flush(self):
        """ method implements stream flush interface, allowing to redirect stdout to logger """
        for handler in self.logger.handlers:
            handler.flush()

    def isatty(self):
        """ is the sys.stdout attached to the terminal?
        python -c "import sys; print(sys.stdout.isatty())" (should write True)
        python -c "import sys; print(sys.stdout.isatty())" | grep . (should write False).
        :return: False, indicating that the output is pipped or redirected
        """
        return False


# holds Logger instance per process name (and optional suffix)
logger_pool = dict()


def get_logger(process_name, append_to_console=settings.settings['under_test']):
    """ method returns initiated logger"""
    if process_name not in logger_pool:
        file_name = get_log_filename(process_name)
        log_tag = get_log_tag(process_name)
        logger_pool[process_name] = Logger(file_name, log_tag, append_to_console=append_to_console)
    return logger_pool[process_name].get_logger()


def get_log_filename(process_name):
    """method returns path for the Log filename"""
    return settings.settings['log_directory'] + context.process_context[process_name].log_filename


def get_log_tag(process_name):
    """method returns tag that all messages will be preceded with"""
    process_obj = context.process_context[process_name]
    if isinstance(process_obj, FreerunProcessEntry):
        return str(process_obj.token)
    elif isinstance(process_obj, ManagedProcessEntry):
        return str(process_obj.token) + str(process_obj.time_qualifier)
    elif isinstance(process_obj, DaemonProcessEntry):
        return str(process_obj.token)
    else:
        raise ValueError('Unknown process type: %s' % process_obj.__class__.__name__)


if __name__ == '__main__':
    process_name = 'TestAggregator'
    logger = get_logger(process_name)
    logger.info('test_message')
    print('regular print message')
    sys.stdout.flush()
