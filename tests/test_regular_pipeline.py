__author__ = 'Bohdan Mushkevych'

from db.error import DuplicateKeyError
from db.model import time_table_record
from db.model.time_table_record import TimeTableRecord
from db.model.unit_of_work import UnitOfWork

import unittest
from mockito import spy, verify, mock, when
from mockito.matchers import any
from system.process_context import PROCESS_UNIT_TEST, ProcessContext
from scheduler.time_table import TimeTable
from scheduler.regular_pipeline import RegularPipeline

TEST_HOURLY_TIMEPERIOD = '2013010122'
TEST_DAILY_TIMEPERIOD = '2013010100'
TEST_MONTHLY_TIMEPERIOD = '2013010000'


class RegularPipelineUnitTest(unittest.TestCase):
    def setUp(self):
        self.logger = ProcessContext.get_logger(PROCESS_UNIT_TEST)
        self.time_table_mocked = mock(TimeTable(self.logger))
        self.pipeline_real = RegularPipeline(self.logger, self.time_table_mocked)

    def tearDown(self):
        pass

    def test_state_embryo(self):
        """ method tests timetable records in STATE_EMBRYO state"""
        def thenReturnUOW(process_name, start_time, end_time, timetable_record):
            return UnitOfWork()

        self.pipeline_real.compute_scope_of_processing = thenReturnUOW
        pipeline = spy(self.pipeline_real)

        from system.process_context import PROCESS_SITE_HOURLY
        timetable_record = TimeTableRecord()
        timetable_record.state = time_table_record.STATE_EMBRYO
        timetable_record.timeperiod = TEST_HOURLY_TIMEPERIOD
        timetable_record.process_name = PROCESS_SITE_HOURLY

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked).\
            update_timetable_record(any(str), any(TimeTableRecord), any(UnitOfWork), any(str))

    def test_duplicatekeyerror_state_embryo(self):
        """ method tests timetable records in STATE_EMBRYO state"""
        def thenRaise(process_name, start_time, end_time, timetable_record):
            raise DuplicateKeyError('Simulated Exception')

        self.pipeline_real.compute_scope_of_processing = thenRaise
        pipeline = spy(self.pipeline_real)

        from system.process_context import PROCESS_SITE_HOURLY
        timetable_record = TimeTableRecord()
        timetable_record.state = time_table_record.STATE_EMBRYO
        timetable_record.timeperiod = TEST_HOURLY_TIMEPERIOD
        timetable_record.process_name = PROCESS_SITE_HOURLY

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked, times=0).\
            update_timetable_record(any(str), any(TimeTableRecord), any(UnitOfWork), any(str))

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
