__author__ = 'Bohdan Mushkevych'

import unittest
try:
    import mock
except ImportError:
    from unittest import mock

from settings import enable_test_mode
enable_test_mode()

from constants import PROCESS_SITE_HOURLY
from synergy.db.dao.job_dao import JobDao
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.model import job, unit_of_work
from synergy.db.manager.ds_manager import BaseManager
from synergy.system.data_logging import get_logger
from synergy.scheduler.timetable import Timetable
from synergy.scheduler.state_machine_recomputing import StateMachineRecomputing
from tests.state_machine_testing_utils import *
from tests.base_fixtures import create_unit_of_work
from tests.ut_context import PROCESS_UNIT_TEST


class RecomputingSMUnitTest(unittest.TestCase):
    def setUp(self):
        self.logger = get_logger(PROCESS_UNIT_TEST)

        self.time_table_mocked = mock.create_autospec(Timetable)
        self.job_dao_mocked = mock.create_autospec(JobDao)
        self.uow_dao_mocked = mock.create_autospec(UnitOfWorkDao)
        self.ds_mocked = mock.create_autospec(BaseManager)

        self.sm_real = StateMachineRecomputing(self.logger, self.time_table_mocked)
        self.sm_real.uow_dao = self.uow_dao_mocked
        self.sm_real.job_dao = self.job_dao_mocked
        self.sm_real.ds = self.ds_mocked
        self.sm_real.update_job = mock.Mock(side_effect=self.sm_real.update_job)
        self.sm_real._process_state_final_run = mock.Mock(side_effect=self.sm_real._process_state_final_run)
        self.sm_real._process_state_in_progress = mock.Mock(side_effect=self.sm_real._process_state_in_progress)
        self.sm_real._compute_and_transfer_to_final_run = \
            mock.Mock(side_effect=self.sm_real._compute_and_transfer_to_final_run)

    def tearDown(self):
        pass

    def test_state_embryo(self):
        """ method tests job records in STATE_EMBRYO state"""
        self.sm_real.insert_and_publish_uow = then_return_uow

        self.ds_mocked.highest_primary_key = mock.MagicMock(return_value=1)
        self.ds_mocked.lowest_primary_key = mock.MagicMock(return_value=0)

        job_record = get_job_record(job.STATE_EMBRYO, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.sm_real.update_job.assert_called_once_with(mock.ANY, mock.ANY, mock.ANY)

    def test_duplicatekeyerror_state_embryo(self):
        """ method tests job records in STATE_EMBRYO state"""
        self.sm_real._insert_uow = then_raise_uw

        job_record = get_job_record(job.STATE_EMBRYO, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        try:
            self.sm_real.manage_job(job_record)
            self.assertTrue(False, 'UserWarning exception should have been thrown')
        except UserWarning:
            self.assertTrue(True)

    def test_future_timeperiod_state_in_progress(self):
        """ method tests job records in STATE_IN_PROGRESS state"""
        job_record = get_job_record(job.STATE_IN_PROGRESS, TEST_FUTURE_TIMEPERIOD, PROCESS_SITE_HOURLY)
        manual_uow = create_unit_of_work(PROCESS_SITE_HOURLY, 0, 1, TEST_ACTUAL_TIMEPERIOD)

        self.uow_dao_mocked.get_one = mock.MagicMock(return_value=manual_uow)
        self.time_table_mocked.is_job_record_finalizable = mock.MagicMock(return_value=True)

        self.sm_real.insert_and_publish_uow = then_return_duplicate_uow

        self.sm_real.manage_job(job_record)
        self.assertEqual(len(self.sm_real.update_job.call_args_list), 0)

    def test_preset_timeperiod_state_in_progress(self):
        """ method tests job records in STATE_IN_PROGRESS state"""
        self.time_table_mocked.is_job_record_finalizable = mock.MagicMock(return_value=True)
        returns = [
            create_unit_of_work(PROCESS_SITE_HOURLY, 1, 1, TEST_ACTUAL_TIMEPERIOD, unit_of_work.STATE_REQUESTED),
            create_unit_of_work(PROCESS_SITE_HOURLY, 1, 1, TEST_ACTUAL_TIMEPERIOD, unit_of_work.STATE_PROCESSED)
        ]

        def side_effects(*args):
            return returns.pop(0)

        self.uow_dao_mocked.get_one = mock.MagicMock(side_effect=side_effects)

        self.ds_mocked.highest_primary_key = mock.MagicMock(return_value=1)
        self.ds_mocked.lowest_primary_key = mock.MagicMock(return_value=0)

        self.sm_real.insert_and_publish_uow = then_return_uow

        job_record = get_job_record(job.STATE_IN_PROGRESS, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.assertEqual(len(self.sm_real.update_job.call_args_list), 1)
        self.assertEqual(len(self.sm_real._compute_and_transfer_to_final_run.call_args_list), 1)
        self.assertEqual(len(self.sm_real._process_state_final_run.call_args_list), 0)

    def test_transfer_to_final_state_from_in_progress(self):
        """ method tests job records in STATE_IN_PROGRESS state"""
        self.time_table_mocked.is_job_record_finalizable = mock.MagicMock(return_value=True)
        returns = [
            create_unit_of_work(PROCESS_SITE_HOURLY, 1, 1, TEST_ACTUAL_TIMEPERIOD, unit_of_work.STATE_REQUESTED),
            create_unit_of_work(PROCESS_SITE_HOURLY, 1, 1, TEST_ACTUAL_TIMEPERIOD, unit_of_work.STATE_PROCESSED)
        ]

        def side_effects(*args):
            return returns.pop(0)

        self.uow_dao_mocked.get_one = mock.MagicMock(side_effect=side_effects)

        self.ds_mocked.highest_primary_key = mock.MagicMock(return_value=1)
        self.ds_mocked.lowest_primary_key = mock.MagicMock(return_value=0)

        self.sm_real.insert_and_publish_uow = then_return_duplicate_uow

        job_record = get_job_record(job.STATE_IN_PROGRESS, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.assertEqual(len(self.sm_real.update_job.call_args_list), 2)
        self.assertEqual(len(self.sm_real._compute_and_transfer_to_final_run.call_args_list), 1)
        self.assertEqual(len(self.sm_real._process_state_final_run.call_args_list), 1)

    def test_processed_state_final_run(self):
        """method tests job records in STATE_FINAL_RUN state"""
        self.uow_dao_mocked.get_one = mock.MagicMock(
            side_effect=lambda *_: create_unit_of_work(
                PROCESS_SITE_HOURLY, 1, 1, TEST_ACTUAL_TIMEPERIOD, unit_of_work.STATE_PROCESSED))

        job_record = get_job_record(job.STATE_FINAL_RUN, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.assertEqual(len(self.sm_real.update_job.call_args_list), 1)
        self.assertEqual(len(self.time_table_mocked.get_tree.call_args_list), 1)

    def test_cancelled_state_final_run(self):
        """method tests job records in STATE_FINAL_RUN state"""
        self.uow_dao_mocked.get_one = mock.MagicMock(
            side_effect=lambda *_: create_unit_of_work(
                PROCESS_SITE_HOURLY, 1, 1, TEST_ACTUAL_TIMEPERIOD, unit_of_work.STATE_CANCELED))

        job_record = get_job_record(job.STATE_FINAL_RUN, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.assertEqual(len(self.sm_real.update_job.call_args_list), 1)
        self.assertEqual(len(self.time_table_mocked.get_tree.call_args_list), 1)

    def test_state_skipped(self):
        """method tests job records in STATE_SKIPPED state"""
        job_record = get_job_record(job.STATE_SKIPPED, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.assertEqual(len(self.sm_real.update_job.call_args_list), 0)
        self.assertEqual(len(self.time_table_mocked.get_tree.call_args_list), 0)

    def test_state_processed(self):
        """method tests job records in STATE_PROCESSED state"""
        job_record = get_job_record(job.STATE_PROCESSED, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.assertEqual(len(self.sm_real.update_job.call_args_list), 0)
        self.assertEqual(len(self.time_table_mocked.get_tree.call_args_list), 0)


if __name__ == '__main__':
    unittest.main()
