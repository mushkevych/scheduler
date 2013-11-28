__author__ = 'Bohdan Mushkevych'

import unittest
from mockito import spy, verify, mock
from system.process_context import PROCESS_UNIT_TEST, PROCESS_GC, ProcessContext
from scheduler.time_table import TimeTable
from scheduler.regular_pipeline import RegularPipeline


class RegularPipelineUnitTest(unittest.TestCase):
    def setUp(self):
        self.logger = ProcessContext.get_logger(PROCESS_UNIT_TEST)
        time_table = mock(TimeTable(self.logger))
        self.pipeline = spy(RegularPipeline(self.logger, time_table))

    def tearDown(self):
        pass

    def test_state_embryo(self):
        """ method tests timetable records in STATE_EMBRYO state"""
        pass

    def test_state_in_progress(self):
        """ method tests timetable records in STATE_IN_PROGRESS state"""
        pass

    def test_state_final_run(self):
        """method tests timetable records in STATE_FINAL_RUN state"""
        pass

    def test_state_skipped(self):
        """method tests timetable records in STATE_FINAL_SKIPPED state"""
        pass

    def test_state_processed(self):
        """method tests timetable records in STATE_FINAL_SKIPPED state"""
        pass


if __name__ == '__main__':
    unittest.main()
