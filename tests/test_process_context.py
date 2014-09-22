__author__ = 'Bohdan Mushkevych'

import unittest

from constants import PROCESS_GC
from supervisor.supervisor_constants import PROCESS_SUPERVISOR
from conf.process_context import ProcessContext


class ProcessContextUnitTest(unittest.TestCase):
    def setUp(self):
        try:
            self.backup_process_name = ProcessContext.get_current_process()
            ProcessContext._current_process_name = ''
        except AttributeError:
            self.backup_process_name = None

    def tearDown(self):
        if self.backup_process_name is not None:
            ProcessContext._current_process_name = self.backup_process_name

    def test_non_initialized(self):
        try:
            ProcessContext.get_current_process()
            self.assertTrue(False, 'get_current_process should throw an exception when current_process was not yet set')
        except AttributeError:
            self.assertTrue(True)

    def test_initialized(self):
        try:
            ProcessContext.set_current_process(PROCESS_GC)
            self.assertEqual(PROCESS_GC, ProcessContext.get_current_process())
        except AttributeError:
            self.assertTrue(False, 'get_current_process should return valid current_process')

    def test_double_initialization(self):
        try:
            ProcessContext.set_current_process(PROCESS_GC)
            ProcessContext.set_current_process(PROCESS_SUPERVISOR)
            self.assertTrue(False, 'set_current_process should not allow double initialization')
        except AttributeError:
            self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
