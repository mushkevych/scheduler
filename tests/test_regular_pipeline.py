from db.dao.unit_of_work_dao import UnitOfWorkDao
from system import time_helper

__author__ = 'Bohdan Mushkevych'

from db.error import DuplicateKeyError
from db.model import time_table_record, unit_of_work
from db.model.time_table_record import TimeTableRecord
from db.model.unit_of_work import UnitOfWork


import unittest
from mockito import spy, verify, mock, when
from mockito.matchers import any
from system.process_context import PROCESS_UNIT_TEST, PROCESS_SITE_HOURLY, ProcessContext
from scheduler.time_table import TimeTable
from scheduler.regular_pipeline import RegularPipeline

TEST_PRESET_TIMEPERIOD = '2013010122'
TEST_ACTUAL_TIMEPERIOD = time_helper.actual_time(PROCESS_SITE_HOURLY)
TEST_FUTURE_TIMEPERIOD = time_helper.increment_time(PROCESS_SITE_HOURLY, TEST_ACTUAL_TIMEPERIOD)


def thenRaise(process_name, start_time, end_time, timetable_record):
    raise DuplicateKeyError('Simulated Exception')


def thenReturnUOW(process_name, start_time, end_time, timetable_record):
    return get_uow(unit_of_work.STATE_REQUESTED)


def get_timetable_record(state, timeperiod, process_name):
    timetable_record = TimeTableRecord()
    timetable_record.state = state
    timetable_record.timeperiod = timeperiod
    timetable_record.process_name = process_name
    timetable_record.document['_id'] = 'alpha_id'
    return timetable_record


def get_uow(state):
    uow = UnitOfWork()
    uow.state = state
    return uow


class RegularPipelineUnitTest(unittest.TestCase):
    def setUp(self):
        self.logger = ProcessContext.get_logger(PROCESS_UNIT_TEST)
        self.time_table_mocked = mock(TimeTable)
        when(self.time_table_mocked).get_tree(any(str)).thenReturn(mock())
        self.pipeline_real = RegularPipeline(self.logger, self.time_table_mocked)

    def tearDown(self):
        pass

    def test_state_embryo(self):
        """ method tests timetable records in STATE_EMBRYO state"""
        self.pipeline_real.compute_scope_of_processing = thenReturnUOW
        pipeline = spy(self.pipeline_real)

        timetable_record = get_timetable_record(time_table_record.STATE_EMBRYO,
                                                TEST_PRESET_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked).\
            update_timetable_record(any(str), any(TimeTableRecord), any(UnitOfWork), any(str))

    def test_duplicatekeyerror_state_embryo(self):
        """ method tests timetable records in STATE_EMBRYO state"""
        self.pipeline_real.compute_scope_of_processing = thenRaise
        pipeline = spy(self.pipeline_real)

        timetable_record = get_timetable_record(time_table_record.STATE_EMBRYO,
                                                TEST_PRESET_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked, times=0).\
            update_timetable_record(any(str), any(TimeTableRecord), any(UnitOfWork), any(str))

    def test_future_timeperiod_state_in_progress(self):
        """ method tests timetable records in STATE_IN_PROGRESS state"""
        when(self.time_table_mocked).can_finalize_timetable_record(any(str), any(TimeTableRecord)).thenReturn(True)
        uow_dao_mock = mock(UnitOfWorkDao)
        when(uow_dao_mock).get_one(any(str)).thenReturn(get_uow(unit_of_work.STATE_REQUESTED))
        self.pipeline_real.uow_dao = uow_dao_mock

        self.pipeline_real.compute_scope_of_processing = thenRaise
        pipeline = spy(self.pipeline_real)

        timetable_record = get_timetable_record(time_table_record.STATE_IN_PROGRESS,
                                                TEST_FUTURE_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked, times=0).\
            update_timetable_record(any(str), any(TimeTableRecord), any(UnitOfWork), any(str))

    def test_preset_timeperiod_state_in_progress(self):
        """ method tests timetable records in STATE_IN_PROGRESS state"""
        when(self.time_table_mocked).can_finalize_timetable_record(any(str), any(TimeTableRecord)).thenReturn(True)
        uow_dao_mock = mock(UnitOfWorkDao)
        when(uow_dao_mock).get_one(any()).\
            thenReturn(get_uow(unit_of_work.STATE_REQUESTED)).\
            thenReturn(get_uow(unit_of_work.STATE_PROCESSED))
        self.pipeline_real.uow_dao = uow_dao_mock

        self.pipeline_real.compute_scope_of_processing = thenReturnUOW
        pipeline = spy(self.pipeline_real)

        timetable_record = get_timetable_record(time_table_record.STATE_IN_PROGRESS,
                                                TEST_PRESET_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked, times=1).\
            update_timetable_record(any(str), any(TimeTableRecord), any(UnitOfWork), any(str))
        # verify(pipeline, times=1).\
        #     _compute_and_transfer_to_final_run(any(str), any(str), any(str), any(TimeTableRecord))
        # verify(pipeline, times=0).\
        #     _process_state_final_run(any(str), any(TimeTableRecord))

    def test_transfer_to_final_timeperiod_state_in_progress(self):
        """ method tests timetable records in STATE_IN_PROGRESS state"""
        when(self.time_table_mocked).can_finalize_timetable_record(any(str), any(TimeTableRecord)).thenReturn(True)
        uow_dao_mock = mock(UnitOfWorkDao)
        when(uow_dao_mock).get_one(any()).\
            thenReturn(get_uow(unit_of_work.STATE_REQUESTED)).\
            thenReturn(get_uow(unit_of_work.STATE_PROCESSED))
        self.pipeline_real.uow_dao = uow_dao_mock

        self.pipeline_real.compute_scope_of_processing = thenRaise
        pipeline = spy(self.pipeline_real)

        timetable_record = get_timetable_record(time_table_record.STATE_IN_PROGRESS,
                                                TEST_PRESET_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked, times=1).\
            update_timetable_record(any(str), any(TimeTableRecord), any(UnitOfWork), any(str))
        # verify(pipeline, times=1).\
        #     _compute_and_transfer_to_final_run(any(str), any(str), any(str), any(TimeTableRecord))
        # verify(pipeline, times=1).\
        #     _process_state_final_run(any(str), any(TimeTableRecord))

    def test_processed_state_final_run(self):
        """method tests timetable records in STATE_FINAL_RUN state"""
        uow_dao_mock = mock(UnitOfWorkDao)
        when(uow_dao_mock).get_one(any()).thenReturn(get_uow(unit_of_work.STATE_PROCESSED))
        self.pipeline_real.uow_dao = uow_dao_mock

        pipeline = spy(self.pipeline_real)

        timetable_record = get_timetable_record(time_table_record.STATE_FINAL_RUN,
                                                TEST_PRESET_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked, times=1). \
            update_timetable_record(any(str), any(TimeTableRecord), any(UnitOfWork), any(str))
        verify(self.time_table_mocked, times=1).get_tree(any(str))

    def test_cancelled_state_final_run(self):
        """method tests timetable records in STATE_FINAL_RUN state"""
        uow_dao_mock = mock(UnitOfWorkDao)
        when(uow_dao_mock).get_one(any()).thenReturn(get_uow(unit_of_work.STATE_CANCELED))
        self.pipeline_real.uow_dao = uow_dao_mock

        pipeline = spy(self.pipeline_real)
        timetable_record = get_timetable_record(time_table_record.STATE_FINAL_RUN,
                                                TEST_PRESET_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked, times=1). \
            update_timetable_record(any(str), any(TimeTableRecord), any(UnitOfWork), any(str))
        verify(self.time_table_mocked, times=0).get_tree(any(str))

    def test_state_skipped(self):
        """method tests timetable records in STATE_SKIPPED state"""
        pipeline = spy(self.pipeline_real)
        timetable_record = get_timetable_record(time_table_record.STATE_SKIPPED,
                                                TEST_PRESET_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked, times=0). \
            update_timetable_record(any(str), any(TimeTableRecord), any(UnitOfWork), any(str))
        verify(self.time_table_mocked, times=0).get_tree(any(str))

    def test_state_processed(self):
        """method tests timetable records in STATE_PROCESSED state"""
        pipeline = spy(self.pipeline_real)
        timetable_record = get_timetable_record(time_table_record.STATE_PROCESSED,
                                                TEST_PRESET_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked, times=0). \
            update_timetable_record(any(str), any(TimeTableRecord), any(UnitOfWork), any(str))
        verify(self.time_table_mocked, times=0).get_tree(any(str))


if __name__ == '__main__':
    unittest.main()
