__author__ = 'Bohdan Mushkevych'

import mock
import unittest
from settings import enable_test_mode
enable_test_mode()

from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.model.job import Job
from synergy.db.model import job, unit_of_work
from synergy.db.manager.ds_manager import BaseManager
from synergy.system import time_helper
from synergy.system.time_qualifier import *
from synergy.system.data_logging import get_logger
from synergy.scheduler.timetable import Timetable
from synergy.scheduler.state_machine_dicrete import StateMachineDiscrete
from constants import PROCESS_SITE_HOURLY
from tests.base_fixtures import create_unit_of_work
from tests.ut_context import PROCESS_UNIT_TEST

TEST_PRESET_TIMEPERIOD = '2013010122'
TEST_ACTUAL_TIMEPERIOD = time_helper.actual_timeperiod(QUALIFIER_HOURLY)
TEST_FUTURE_TIMEPERIOD = time_helper.increment_timeperiod(QUALIFIER_HOURLY, TEST_ACTUAL_TIMEPERIOD)


def then_raise(process_name, start_timeperiod, end_timeperiod, start_id, end_id):
    raise UserWarning('Simulated UserWarning Exception')


def then_return_uow(process_name, start_timeperiod, end_timeperiod, start_id, end_id):
    return create_unit_of_work(process_name, start_id, end_id, start_timeperiod), False


def then_return_duplicate_uow(process_name, start_timeperiod, end_timeperiod, start_id, end_id):
    return create_unit_of_work(process_name, start_id, end_id, start_timeperiod), True


def get_job_record(state, timeperiod, process_name):
    job_record = Job()
    job_record.state = state
    job_record.timeperiod = timeperiod
    job_record.process_name = process_name
    job_record.db_id = 'alpha_id'
    return job_record


class DiscreteSMUnitTest(unittest.TestCase):
    def setUp(self):
        self.logger = get_logger(PROCESS_UNIT_TEST)
        self.time_table_mocked = mock.create_autospec(Timetable)
        self.sm_real = StateMachineDiscrete(self.logger, self.time_table_mocked)

        self.uow_dao_mocked = mock.create_autospec(UnitOfWorkDao)
        self.sm_real.uow_dao = self.uow_dao_mocked

        self.ds_mocked = mock.create_autospec(BaseManager)
        self.sm_real.ds = self.ds_mocked

    def tearDown(self):
        pass

    def test_state_embryo(self):
        """ method tests job records in STATE_EMBRYO state"""
        self.sm_real.insert_and_publish_uow = then_return_uow

        self.ds_mocked.highest_primary_key = mock.MagicMock(return_value=1)
        self.ds_mocked.lowest_primary_key = mock.MagicMock(return_value=0)

        job_record = get_job_record(job.STATE_EMBRYO, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.time_table_mocked.update_job_record.assert_called_once_with(mock.ANY, mock.ANY, mock.ANY)

    def test_duplicatekeyerror_state_embryo(self):
        """ method tests job records in STATE_EMBRYO state"""
        self.sm_real._insert_uow = then_raise

        job_record = get_job_record(job.STATE_EMBRYO, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        try:
            self.sm_real.manage_job(job_record)
            self.assertTrue(False, 'UserWarning exception should have been thrown')
        except UserWarning:
            self.assertTrue(True)

    def test_future_timeperiod_state_in_progress(self):
        """ method tests timetable records in STATE_IN_PROGRESS state"""
        job_record = get_job_record(job.STATE_IN_PROGRESS, TEST_FUTURE_TIMEPERIOD, PROCESS_SITE_HOURLY)
        manual_uow = create_unit_of_work(PROCESS_SITE_HOURLY, 0, 1, None)

        self.uow_dao_mocked.get_one = mock.MagicMock(return_value=manual_uow)
        self.time_table_mocked.is_job_record_finalizable = mock.MagicMock(return_value=True)

        self.sm_real.insert_and_publish_uow = then_raise

        self.sm_real.manage_job(job_record)
        self.assertTrue(self.time_table_mocked.update_job_record.call_args_list == [])  # called 0 times

    def test_preset_timeperiod_state_in_progress(self):
        """ method tests timetable records in STATE_IN_PROGRESS state"""
        self.time_table_mocked.is_job_record_finalizable = mock.MagicMock(return_value=True)
        self.uow_dao_mocked.get_one = mock.MagicMock(
            side_effect=create_unit_of_work(PROCESS_SITE_HOURLY, 0, 1, None))

        self.sm_real.insert_and_publish_uow = then_return_uow

        job_record = get_job_record(job.STATE_IN_PROGRESS, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.assertEqual(len(self.time_table_mocked.update_job_record.call_args_list), 0)
        self.assertEqual(len(self.time_table_mocked._compute_and_transfer_to_final_run.call_args_list), 1)
        self.assertEqual(len(self.time_table_mocked._process_state_final_run.call_args_list), 0)

    def test_transfer_to_final_state_from_in_progress(self):
        """ method tests timetable records in STATE_IN_PROGRESS state"""
        self.time_table_mocked.is_job_record_finalizable = mock.MagicMock(return_value=True)
        self.uow_dao_mocked.get_one = mock.MagicMock(
            side_effect=create_unit_of_work(PROCESS_SITE_HOURLY, 1, 1, None, unit_of_work.STATE_PROCESSED))

        self.sm_real.insert_and_publish_uow = then_return_duplicate_uow

        job_record = get_job_record(job.STATE_IN_PROGRESS, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.assertEqual(len(self.time_table_mocked.update_job_record.call_args_list), 1)
        self.assertEqual(len(self.time_table_mocked._compute_and_transfer_to_final_run.call_args_list), 1)
        self.assertEqual(len(self.time_table_mocked._process_state_final_run.call_args_list), 1)

    def test_retry_state_in_progress(self):
        """ method tests timetable records in STATE_IN_PROGRESS state"""
        self.time_table_mocked.is_job_record_finalizable = mock.MagicMock(return_value=True)
        self.uow_dao_mocked.get_one = mock.MagicMock(
            side_effect=create_unit_of_work(PROCESS_SITE_HOURLY, 1, 1, None, unit_of_work.STATE_PROCESSED))

        self.sm_real.insert_and_publish_uow = then_return_uow

        job_record = get_job_record(job.STATE_IN_PROGRESS, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.assertEqual(len(self.time_table_mocked.update_job_record.call_args_list), 1)
        self.assertEqual(len(self.time_table_mocked._compute_and_transfer_to_final_run.call_args_list), 1)
        self.assertEqual(len(self.time_table_mocked._process_state_final_run.call_args_list), 1)

    def test_processed_state_final_run(self):
        """method tests timetable records in STATE_FINAL_RUN state"""
        self.uow_dao_mocked.get_one = mock.MagicMock(
            side_effect=create_unit_of_work(PROCESS_SITE_HOURLY, 1, 1, None, unit_of_work.STATE_PROCESSED))

        job_record = get_job_record(job.STATE_FINAL_RUN, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.assertEqual(len(self.time_table_mocked.update_job_record.call_args_list), 1)
        self.assertEqual(len(self.time_table_mocked.get_tree.call_args_list), 1)

    def test_cancelled_state_final_run(self):
        """method tests timetable records in STATE_FINAL_RUN state"""
        self.uow_dao_mocked.get_one = mock.MagicMock(
            side_effect=create_unit_of_work(PROCESS_SITE_HOURLY, 1, 1, None, unit_of_work.STATE_CANCELED))

        job_record = get_job_record(job.STATE_FINAL_RUN, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.assertEqual(len(self.time_table_mocked.update_job_record.call_args_list), 1)
        self.assertEqual(len(self.time_table_mocked.get_tree.call_args_list), 1)

    def test_state_skipped(self):
        """method tests timetable records in STATE_SKIPPED state"""
        job_record = get_job_record(job.STATE_SKIPPED, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.assertEqual(len(self.time_table_mocked.update_job_record.call_args_list), 0)
        self.assertEqual(len(self.time_table_mocked.get_tree.call_args_list), 0)

    def test_state_processed(self):
        """method tests timetable records in STATE_PROCESSED state"""
        job_record = get_job_record(job.STATE_PROCESSED, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.assertEqual(len(self.time_table_mocked.update_job_record.call_args_list), 0)
        self.assertEqual(len(self.time_table_mocked.get_tree.call_args_list), 0)


if __name__ == '__main__':
    unittest.main()
