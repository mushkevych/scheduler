__author__ = 'Bohdan Mushkevych'

import mock
import unittest

from settings import enable_test_mode
enable_test_mode()

from constants import PROCESS_SITE_HOURLY
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.system.data_logging import get_logger
from synergy.scheduler.timetable import Timetable
from synergy.scheduler.abstract_state_machine import AbstractStateMachine
from tests.state_machine_testing_utils import *
from tests.ut_context import PROCESS_UNIT_TEST


class AbstractSMUnitTest(unittest.TestCase):
    def setUp(self):
        self.logger = get_logger(PROCESS_UNIT_TEST)
        self.time_table_mocked = mock.create_autospec(Timetable)
        self.uow_dao_mocked = mock.create_autospec(UnitOfWorkDao)
        self.sm_real = AbstractStateMachine(self.logger, self.time_table_mocked, 'AbstractStateMachine')
        self.sm_real.uow_dao = self.uow_dao_mocked

    def tearDown(self):
        pass

    def test_insert_and_publish_uow(self):
        """ method tests happy-flow for insert_and_publish_uow method """
        self.sm_real._insert_uow = then_return_uow
        self.sm_real._publish_uow = mock.MagicMock(return_value=True)

        uow, is_duplicate = self.sm_real.insert_and_publish_uow(PROCESS_SITE_HOURLY,
                                                                TEST_PRESET_TIMEPERIOD,
                                                                None, 0, 1)
        manual_uow = then_return_uow(PROCESS_SITE_HOURLY, TEST_PRESET_TIMEPERIOD, None, 0, 1)
        self.assertFalse(is_duplicate)
        self.assertDictEqual(uow.document, manual_uow.document)

    def test_unhandled_exception_iapu(self):
        """ method tests unhandled UserWarning exception at insert_and_publish_uow method """
        self.sm_real._insert_uow = then_raise_dpk
        self.sm_real._publish_uow = mock.MagicMock(return_value=True)

        try:
            self.sm_real.insert_and_publish_uow(PROCESS_SITE_HOURLY, TEST_PRESET_TIMEPERIOD, None, 0, 1)
            self.assertTrue(False, 'UserWarning exception should have been thrown')
        except UserWarning:
            self.assertTrue(True)

    def test_handled_exception_iapu(self):
        """ method tests handled UserWarning exception at insert_and_publish_uow method """
        manual_uow = then_return_uow(PROCESS_SITE_HOURLY, TEST_PRESET_TIMEPERIOD, None, 0, 1)

        self.sm_real._insert_uow = then_raise_dpk
        self.sm_real._publish_uow = mock.MagicMock(return_value=True)

        self.uow_dao_mocked.recover_from_duplicatekeyerror = mock.MagicMock(return_value=manual_uow)
        uow, is_duplicate = self.sm_real.insert_and_publish_uow(PROCESS_SITE_HOURLY,
                                                                TEST_PRESET_TIMEPERIOD,
                                                                None, 0, 1)
        self.assertTrue(is_duplicate)
        self.assertDictEqual(uow.document, manual_uow.document)


if __name__ == '__main__':
    unittest.main()
