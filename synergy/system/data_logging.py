__author__ = 'Bohdan Mushkevych'

import sys
import logging
import logging.handlers

from synergy.conf import settings


class Logger(object):
    """
    Logger presents standard API to log messages and store them for future analysis
    """

    def __init__(self, file_name, context):
        """
        Constructor: dictionary of loggers available for this Python process
        @param file_name: path+name of the output file
        @param context: tag that is printed ahead of every logged message
        """
        self.logger = logging.getLogger(context)

        if settings.settings['under_test']:
            # ATTENTION: while running as stand-alone process, stdout and stderr must be muted and redirected to file
            # otherwise the their pipes get overfilled, and process halts
            stream_handler = logging.StreamHandler()
            stream_formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
            stream_handler.setFormatter(stream_formatter)
            self.logger.addHandler(stream_handler)

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

        # While under_test, tools as xml_unittest_runner are doing complex sys.stdXXX reassignments
        if not settings.settings['under_test']:
            sys.stdout = self
            sys.stderr = self

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
        @return: False, indicating that the output is pipped or redirected
        """
        return False


if __name__ == '__main__':
    from synergy.conf.process_context import ProcessContext

    process_name = 'TestAggregator'
    logger = ProcessContext.get_logger(process_name)
    logger.info('test_message')
    print 'regular print message'
    sys.stdout.flush()
