from synergy.system import time_helper

__author__ = 'Bohdan Mushkevych'

import os
import random
import shutil
import string
import tempfile
import unittest
import socket

from synergy.conf import settings
from synergy.workers.abstract_file_collector_worker import AbstractFileCollectorWorker
from synergy.conf.process_context import ProcessContext
from synergy.system.time_qualifier import *
from tests.ut_context import PROCESS_UNIT_TEST


def string_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


class DummyFileCollector(AbstractFileCollectorWorker):

    def _get_file_pattern(self, timeperiod):
        return '%s-*.gz' % timeperiod

    def _get_source_folder(self):
        return settings.settings['remote_source_folder']


class FileCollectorUnitTest(unittest.TestCase):
    """
    Following steps are required at OS-level to run this Unit Test on local box:
    1. ssh-keygen -t rsa
    Press enter for each line
    2. cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    3. chmod 600 ~/.ssh/authorized_keys

    To run this unit test against remote box, it has to contain public .ssh keys:

    1. cat ~/.ssh/id_rsa.pub
    copy that key

    On remote machine:

    1. mkdir ~/.ssh && chmod 700 ~/.ssh
    2. touch ~/.ssh/authorized_keys2 && chmod 600 ~/.ssh/authorized_keys2
    3. Paste copied key into authorized_keys2

    """
    ACTUAL_TIMEPERIOD = time_helper.actual_timeperiod(QUALIFIER_HOURLY)
    TABLES = ['table_alpha', 'table_beta', 'table_gama']
    TEST_FILE_SIZE = 1024
    TEST_FILE_LIST = []
    TEST_HEADER_LIST = []

    for table_name in TABLES:
        TEST_FILE_LIST += [ACTUAL_TIMEPERIOD + '-' + table_name + '-host_%r.domain.com.log.gz' % i for i in range(10)]
        TEST_HEADER_LIST += [table_name + '-host_%r.domain.com.header' % i for i in range(10)]

    def create_file_collector(self):
        return DummyFileCollector(PROCESS_UNIT_TEST)

    def setUp(self):
        self.original_source_host_list = settings['remote_source_host_list']
        self.original_source_folder = settings['remote_source_folder']

        self.logger = ProcessContext.get_logger(PROCESS_UNIT_TEST)
        self.worker = self.create_file_collector()
        self.actual_timeperiod = self.ACTUAL_TIMEPERIOD

        # switch off auxiliary threads
        self.worker.performance_ticker.cancel()

        self.worker._create_directories()
        self.tempdir_copying = tempfile.mkdtemp()

        fqsf = os.path.join(self.tempdir_copying, self.actual_timeperiod[:-2])
        if not os.path.exists(fqsf):
            os.makedirs(fqsf)

        fqhf = os.path.join(self.tempdir_copying, AbstractFileCollectorWorker.HEADER_FOLDER)
        if not os.path.exists(fqhf):
            os.makedirs(fqhf)

        for file_name in self.TEST_FILE_LIST:
            output = open(os.path.join(fqsf, file_name), 'w')
            output.write(string_generator(self.TEST_FILE_SIZE))
            output.close()

        for file_name in self.TEST_HEADER_LIST:
            output = open(os.path.join(fqhf, file_name), 'w')
            output.write(','.join(['column_%r' % x for x in range(5)]))
            output.close()

        settings['remote_source_host_list'] = {socket.getfqdn(): ''}
        settings['remote_source_folder'] = self.tempdir_copying

    def tearDown(self):
        settings['remote_source_host_list'] = self.original_source_host_list
        settings['remote_source_folder'] = self.original_source_folder

        # killing the worker
        self.worker.performance_ticker.cancel()
        del self.worker

        if self.tempdir_copying:
            self.logger.info('Cleaning up %r' % self.tempdir_copying)
            shutil.rmtree(self.tempdir_copying, True)
            self.tempdir_copying = None

    def test_copying(self):
        # remote_source_host_list defines where the source files are located
        copied_files = self.worker.copy_archives_from_source(self.actual_timeperiod)
        self.assertEqual(len(copied_files), len(self.TEST_FILE_LIST))


if __name__ == '__main__':
    unittest.main()
