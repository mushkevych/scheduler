__author__ = 'Bohdan Mushkevych'

import mock
import unittest
from settings import enable_test_mode
enable_test_mode()

from constants import PROCESS_SITE_HOURLY
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.model import job, unit_of_work
from synergy.db.manager.ds_manager import BaseManager
from synergy.system.data_logging import get_logger
from synergy.scheduler.timetable import Timetable
from synergy.scheduler.state_machine_simple_dicrete import StateMachineSimpleDiscrete
from tests.state_machine_testing_utils import *
from tests.base_fixtures import create_unit_of_work
from tests.ut_context import PROCESS_UNIT_TEST


class SimpleDiscreteSMUnitTest(unittest.TestCase):
    def setUp(self):
        self.logger = get_logger(PROCESS_UNIT_TEST)

        self.time_table_mocked = mock.create_autospec(Timetable)
        self.uow_dao_mocked = mock.create_autospec(UnitOfWorkDao)
        self.ds_mocked = mock.create_autospec(BaseManager)

        self.sm_real = StateMachineSimpleDiscrete(self.logger, self.time_table_mocked)
        self.sm_real.uow_dao = self.uow_dao_mocked
        self.sm_real.ds = self.ds_mocked
        self.sm_real._process_state_final_run = mock.Mock(
            side_effect=self.sm_real._process_state_final_run)
        self.sm_real._process_state_in_progress = mock.Mock(
            side_effect=self.sm_real._process_state_in_progress)

    def tearDown(self):
        pass

    def test_future_timeperiod_state_in_progress(self):
        """ method tests timetable records in STATE_IN_PROGRESS state"""
        job_record = get_job_record(job.STATE_IN_PROGRESS, TEST_FUTURE_TIMEPERIOD, PROCESS_SITE_HOURLY)
        manual_uow = create_unit_of_work(PROCESS_SITE_HOURLY, 0, 1, None)

        self.uow_dao_mocked.get_one = mock.MagicMock(return_value=manual_uow)
        self.time_table_mocked.is_job_record_finalizable = mock.MagicMock(return_value=True)

        self.sm_real.insert_and_publish_uow = then_raise_uw

        self.sm_real.manage_job(job_record)
        self.assertTrue(self.time_table_mocked.update_job_record.call_args_list == [])  # called 0 times

    def test_preset_timeperiod_state_in_progress(self):
        """ method tests timetable records in STATE_IN_PROGRESS state"""
        self.time_table_mocked.is_job_record_finalizable = mock.MagicMock(return_value=True)
        self.uow_dao_mocked.get_one = mock.MagicMock(
            side_effect=lambda *_: create_unit_of_work(PROCESS_SITE_HOURLY, 0, 1, None))

        self.sm_real.insert_and_publish_uow = then_return_uow

        job_record = get_job_record(job.STATE_IN_PROGRESS, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.assertEqual(len(self.time_table_mocked.update_job_record.call_args_list), 0)
        self.assertEqual(len(self.sm_real._process_state_final_run.call_args_list), 0)

    def test_transfer_to_final_state_from_in_progress(self):
        """ method tests timetable records in STATE_IN_PROGRESS state"""
        self.time_table_mocked.is_job_record_finalizable = mock.MagicMock(return_value=True)
        self.uow_dao_mocked.get_one = mock.MagicMock(
            side_effect=lambda *_: create_unit_of_work(PROCESS_SITE_HOURLY, 1, 1, None, unit_of_work.STATE_PROCESSED))

        self.sm_real.insert_and_publish_uow = then_return_duplicate_uow

        job_record = get_job_record(job.STATE_IN_PROGRESS, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.assertEqual(len(self.time_table_mocked.update_job_record.call_args_list), 1)
        self.assertEqual(len(self.sm_real._process_state_in_progress.call_args_list), 1)
        self.assertEqual(len(self.sm_real._process_state_final_run.call_args_list), 0)

    def test_retry_state_in_progress(self):
        """ method tests timetable records in STATE_IN_PROGRESS state"""
        self.time_table_mocked.is_job_record_finalizable = mock.MagicMock(return_value=True)
        self.uow_dao_mocked.get_one = mock.MagicMock(
            side_effect=lambda *_: create_unit_of_work(PROCESS_SITE_HOURLY, 1, 1, None, unit_of_work.STATE_PROCESSED))

        self.sm_real.insert_and_publish_uow = then_return_uow

        job_record = get_job_record(job.STATE_IN_PROGRESS, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)
        self.sm_real.manage_job(job_record)

        self.assertEqual(len(self.time_table_mocked.update_job_record.call_args_list), 1)
        self.assertEqual(len(self.sm_real._process_state_in_progress.call_args_list), 1)
        self.assertEqual(len(self.sm_real._process_state_final_run.call_args_list), 0)


if __name__ == '__main__':
    unittest.main()
