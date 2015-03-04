__author__ = 'Bohdan Mushkevych'

import unittest

from settings import enable_test_mode
enable_test_mode()

from mockito import spy, mock, when
from mockito.matchers import any

from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.error import DuplicateKeyError
from synergy.db.model.job import Job
from tests.base_fixtures import create_unit_of_work
from synergy.system import time_helper
from synergy.system.time_qualifier import *
from synergy.system.data_logging import get_logger
from constants import PROCESS_SITE_HOURLY
from synergy.scheduler.timetable import Timetable
from synergy.scheduler.abstract_state_machine import AbstractStateMachine
from tests.ut_context import PROCESS_UNIT_TEST

TEST_PRESET_TIMEPERIOD = '2013010122'
TEST_ACTUAL_TIMEPERIOD = time_helper.actual_timeperiod(QUALIFIER_HOURLY)
TEST_FUTURE_TIMEPERIOD = time_helper.increment_timeperiod(QUALIFIER_HOURLY, TEST_ACTUAL_TIMEPERIOD)


def then_raise(process_name, start_timeperiod, end_timeperiod, start_id, end_id):
    raise DuplicateKeyError(process_name, start_timeperiod, start_id, end_id, 'Simulated Exception')


def then_return_uow(process_name, start_timeperiod, end_timeperiod, start_id, end_id):
    return create_unit_of_work(process_name, start_id, end_id, start_timeperiod, uow_id='a_uow_id')


def get_job_record(state, timeperiod, process_name):
    job_record = Job()
    job_record.state = state
    job_record.timeperiod = timeperiod
    job_record.process_name = process_name
    job_record.db_id = 'alpha_id'
    return job_record


class AbstractSMUnitTest(unittest.TestCase):
    def setUp(self):
        self.logger = get_logger(PROCESS_UNIT_TEST)
        self.time_table_mocked = mock(Timetable)
        self.uow_dao_mocked = mock(UnitOfWorkDao)
        self.sm_real = AbstractStateMachine(self.logger, self.time_table_mocked, 'AbstractStateMachine')
        self.sm_real.uow_dao = self.uow_dao_mocked

    def tearDown(self):
        pass

    def test_insert_and_publish_uow(self):
        """ method tests happy-flow for insert_and_publish_uow method """
        self.sm_real._insert_uow = then_return_uow
        when(self.sm_real)._publish_uow(any(object)).thenReturn(True)

        sm_spy = spy(self.sm_real)
        uow, is_duplicate = sm_spy.insert_and_publish_uow(PROCESS_SITE_HOURLY,
                                                          TEST_PRESET_TIMEPERIOD,
                                                          None, 0, 1)
        manual_uow = then_return_uow(PROCESS_SITE_HOURLY, TEST_PRESET_TIMEPERIOD, None, 0, 1)
        self.assertFalse(is_duplicate)
        self.assertDictEqual(uow.document, manual_uow.document)

    def test_unhandled_exception_iapu(self):
        """ method tests unhandled UserWarning exception at insert_and_publish_uow method """
        self.sm_real._insert_uow = then_raise
        when(self.sm_real)._publish_uow(any(object)).thenReturn(True)

        sm_spy = spy(self.sm_real)
        try:
            sm_spy.insert_and_publish_uow(PROCESS_SITE_HOURLY, TEST_PRESET_TIMEPERIOD, None, 0, 1)
            self.assertTrue(False, 'UserWarning exception should have been thrown')
        except UserWarning:
            self.assertTrue(True)

    def test_handled_exception_iapu(self):
        """ method tests handled UserWarning exception at insert_and_publish_uow method """
        manual_uow = then_return_uow(PROCESS_SITE_HOURLY, TEST_PRESET_TIMEPERIOD, None, 0, 1)

        self.sm_real._insert_uow = then_raise
        when(self.sm_real)._publish_uow(any(object)).thenReturn(True)
        when(self.uow_dao_mocked).recover_from_duplicatekeyerror(any(object)).thenReturn(manual_uow)

        sm_spy = spy(self.sm_real)
        uow, is_duplicate = sm_spy.insert_and_publish_uow(PROCESS_SITE_HOURLY,
                                                          TEST_PRESET_TIMEPERIOD,
                                                          None, 0, 1)
        self.assertTrue(is_duplicate)
        self.assertDictEqual(uow.document, manual_uow.document)


if __name__ == '__main__':
    unittest.main()