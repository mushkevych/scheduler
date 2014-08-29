__author__ = 'Bohdan Mushkevych'

from db.dao.unit_of_work_dao import UnitOfWorkDao
from db.error import DuplicateKeyError
from db.model import job, unit_of_work
from db.model.job import Job
from db.model.unit_of_work import UnitOfWork

import unittest
from tests.base_fixtures import create_unit_of_work
from mockito import spy, verify, mock, when
from mockito.matchers import any
from system import time_helper
from system.process_context import ProcessContext, PROCESS_SITE_HOURLY
from scheduler.time_table import Timetable
from scheduler.continuous_pipeline import ContinuousPipeline
from tests.ut_context import PROCESS_UNIT_TEST

TEST_PRESET_TIMEPERIOD = '2013010122'
TEST_ACTUAL_TIMEPERIOD = time_helper.actual_timeperiod(QUALIFIER_HOURLY)
TEST_FUTURE_TIMEPERIOD = time_helper.increment_timeperiod(QUALIFIER_HOURLY, TEST_ACTUAL_TIMEPERIOD)


def then_raise(process_name, start_timeperiod, end_timeperiod, timetable_record):
    exc = DuplicateKeyError('Simulated Exception')
    exc.start_id = '0'
    exc.end_id = '1'
    exc.process_name = process_name
    exc.timeperiod = start_timeperiod
    raise exc


def then_return_uow(process_name, start_timeperiod, end_timeperiod, timetable_record):
    return create_unit_of_work(PROCESS_UNIT_TEST, 0, 1, None)


def get_timetable_record(state, timeperiod, process_name):
    timetable_record = Job()
    timetable_record.state = state
    timetable_record.timeperiod = timeperiod
    timetable_record.process_name = process_name
    timetable_record.document['_id'] = 'alpha_id'
    return timetable_record


class RegularPipelineUnitTest(unittest.TestCase):
    def setUp(self):
        self.logger = ProcessContext.get_logger(PROCESS_UNIT_TEST)
        self.time_table_mocked = mock(Timetable)
        when(self.time_table_mocked).get_tree(any(str)).thenReturn(mock())
        self.pipeline_real = ContinuousPipeline(self.logger, self.time_table_mocked)

    def tearDown(self):
        pass

    def test_state_embryo(self):
        """ method tests timetable records in STATE_EMBRYO state"""
        self.pipeline_real.compute_scope_of_processing = then_return_uow
        pipeline = spy(self.pipeline_real)

        timetable_record = get_timetable_record(job.STATE_EMBRYO,
                                                TEST_PRESET_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked).\
            update_timetable_record(any(str), any(Job), any(UnitOfWork), any(str))

    def test_duplicatekeyerror_state_embryo(self):
        """ method tests timetable records in STATE_EMBRYO state"""
        self.pipeline_real.compute_scope_of_processing = then_raise
        pipeline = spy(self.pipeline_real)

        timetable_record = get_timetable_record(job.STATE_EMBRYO,
                                                TEST_PRESET_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked, times=0).\
            update_timetable_record(any(str), any(Job), any(UnitOfWork), any(str))

    def test_future_timeperiod_state_in_progress(self):
        """ method tests timetable records in STATE_IN_PROGRESS state"""
        when(self.time_table_mocked).can_finalize_timetable_record(any(str), any(Job)).thenReturn(True)
        uow_dao_mock = mock(UnitOfWorkDao)
        when(uow_dao_mock).get_one(any(str)).thenReturn(create_unit_of_work(PROCESS_UNIT_TEST, 0, 1, None))
        self.pipeline_real.uow_dao = uow_dao_mock

        self.pipeline_real.compute_scope_of_processing = then_raise
        pipeline = spy(self.pipeline_real)

        timetable_record = get_timetable_record(job.STATE_IN_PROGRESS,
                                                TEST_FUTURE_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked, times=0).\
            update_timetable_record(any(str), any(Job), any(UnitOfWork), any(str))

    def test_preset_timeperiod_state_in_progress(self):
        """ method tests timetable records in STATE_IN_PROGRESS state"""
        when(self.time_table_mocked).can_finalize_timetable_record(any(str), any(Job)).thenReturn(True)
        uow_dao_mock = mock(UnitOfWorkDao)
        when(uow_dao_mock).get_one(any()).\
            thenReturn(create_unit_of_work(PROCESS_UNIT_TEST, 1, 1, None, unit_of_work.STATE_REQUESTED)).\
            thenReturn(create_unit_of_work(PROCESS_UNIT_TEST, 1, 1, None, unit_of_work.STATE_PROCESSED))
        self.pipeline_real.uow_dao = uow_dao_mock

        self.pipeline_real.compute_scope_of_processing = then_return_uow
        pipeline = spy(self.pipeline_real)

        timetable_record = get_timetable_record(job.STATE_IN_PROGRESS,
                                                TEST_PRESET_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked, times=1).\
            update_timetable_record(any(str), any(Job), any(UnitOfWork), any(str))
        # verify(pipeline, times=1).\
        #     _compute_and_transfer_to_final_run(any(str), any(str), any(str), any(TimeTableRecord))
        # verify(pipeline, times=0).\
        #     _process_state_final_run(any(str), any(TimeTableRecord))

    def test_transfer_to_final_timeperiod_state_in_progress(self):
        """ method tests timetable records in STATE_IN_PROGRESS state"""
        when(self.time_table_mocked).can_finalize_timetable_record(any(str), any(Job)).thenReturn(True)
        uow_dao_mock = mock(UnitOfWorkDao)
        when(uow_dao_mock).get_one(any()).\
            thenReturn(create_unit_of_work(PROCESS_UNIT_TEST, 1, 1, None, unit_of_work.STATE_REQUESTED)).\
            thenReturn(create_unit_of_work(PROCESS_UNIT_TEST, 1, 1, None, unit_of_work.STATE_PROCESSED))
        self.pipeline_real.uow_dao = uow_dao_mock

        self.pipeline_real.compute_scope_of_processing = then_raise
        pipeline = spy(self.pipeline_real)

        timetable_record = get_timetable_record(job.STATE_IN_PROGRESS,
                                                TEST_PRESET_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked, times=1).\
            update_timetable_record(any(str), any(Job), any(UnitOfWork), any(str))
        # verify(pipeline, times=1).\
        #     _compute_and_transfer_to_final_run(any(str), any(str), any(str), any(TimeTableRecord))
        # verify(pipeline, times=1).\
        #     _process_state_final_run(any(str), any(TimeTableRecord))

    def test_processed_state_final_run(self):
        """method tests timetable records in STATE_FINAL_RUN state"""
        uow_dao_mock = mock(UnitOfWorkDao)
        when(uow_dao_mock).get_one(any()).thenReturn(
            create_unit_of_work(PROCESS_UNIT_TEST, 1, 1, None, unit_of_work.STATE_PROCESSED))
        self.pipeline_real.uow_dao = uow_dao_mock

        pipeline = spy(self.pipeline_real)

        timetable_record = get_timetable_record(job.STATE_FINAL_RUN,
                                                TEST_PRESET_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked, times=1). \
            update_timetable_record(any(str), any(Job), any(UnitOfWork), any(str))
        verify(self.time_table_mocked, times=1).get_tree(any(str))

    def test_cancelled_state_final_run(self):
        """method tests timetable records in STATE_FINAL_RUN state"""
        uow_dao_mock = mock(UnitOfWorkDao)
        when(uow_dao_mock).get_one(any()).thenReturn(
            create_unit_of_work(PROCESS_UNIT_TEST, 1, 1, None, unit_of_work.STATE_CANCELED))
        self.pipeline_real.uow_dao = uow_dao_mock

        pipeline = spy(self.pipeline_real)
        timetable_record = get_timetable_record(job.STATE_FINAL_RUN,
                                                TEST_PRESET_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked, times=1). \
            update_timetable_record(any(str), any(Job), any(UnitOfWork), any(str))
        verify(self.time_table_mocked, times=0).get_tree(any(str))

    def test_state_skipped(self):
        """method tests timetable records in STATE_SKIPPED state"""
        pipeline = spy(self.pipeline_real)
        timetable_record = get_timetable_record(job.STATE_SKIPPED,
                                                TEST_PRESET_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked, times=0). \
            update_timetable_record(any(str), any(Job), any(UnitOfWork), any(str))
        verify(self.time_table_mocked, times=0).get_tree(any(str))

    def test_state_processed(self):
        """method tests timetable records in STATE_PROCESSED state"""
        pipeline = spy(self.pipeline_real)
        timetable_record = get_timetable_record(job.STATE_PROCESSED,
                                                TEST_PRESET_TIMEPERIOD,
                                                PROCESS_SITE_HOURLY)

        pipeline.manage_pipeline_for_process(timetable_record.process_name, timetable_record)
        verify(self.time_table_mocked, times=0). \
            update_timetable_record(any(str), any(Job), any(UnitOfWork), any(str))
        verify(self.time_table_mocked, times=0).get_tree(any(str))


if __name__ == '__main__':
    unittest.main()
