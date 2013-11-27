__author__ = 'Bohdan Mushkevych'

import unittest
from mockito import spy, verify
from system.process_context import PROCESS_SUPERVISOR, PROCESS_GC, ProcessContext


class RegularPipelineUnitTest(unittest.TestCase):
    def setUp(self):
        try:
            self.backup_process_name = ProcessContext.get_current_process()
            del ProcessContext.__dict__[ProcessContext._ProcessContext__CURRENT_PROCESS_TAG]
        except AttributeError:
            self.backup_process_name = None

    def tearDown(self):
        if self.backup_process_name is not None:
            ProcessContext.__dict__[ProcessContext._ProcessContext__CURRENT_PROCESS_TAG] = self.backup_process_name

    def test_non_initialized(self):
        try:
            ProcessContext.get_current_process()
            assert False, 'get_current_process should throw an exception when current_process was not yet set'
        except AttributeError:
            assert True

    def test_initialized(self):
        try:
            ProcessContext.set_current_process(PROCESS_GC)
            assert PROCESS_GC == ProcessContext.get_current_process()
        except AttributeError:
            assert False, 'get_current_process should return valid current_process'

    def test_double_initialization(self):
        try:
            ProcessContext.set_current_process(PROCESS_GC)
            ProcessContext.set_current_process(PROCESS_SUPERVISOR)
            assert False, 'set_current_process should not allow double initialization'
        except AttributeError:
            assert True


if __name__ == '__main__':
    unittest.main()
