__author__ = 'Bohdan Mushkevych'

import unittest
try:
    import mock
except ImportError:
    from unittest import mock

from settings import enable_test_mode
enable_test_mode()

from constants import PROCESS_SITE_HOURLY, PROCESS_SITE_DAILY, PROCESS_SITE_MONTHLY, PROCESS_SITE_YEARLY
from synergy.conf import context
from synergy.db.model import job
from synergy.db.dao.job_dao import JobDao
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.system.system_logger import get_logger
from synergy.scheduler.timetable import Timetable
from synergy.system.timeperiod_dict import TimeperiodDict
from synergy.scheduler.abstract_state_machine import AbstractStateMachine
from tests.state_machine_testing_utils import *
from tests.ut_context import PROCESS_UNIT_TEST


class AbstractSMUnitTest(unittest.TestCase):
    def setUp(self):
        self.logger = get_logger(PROCESS_UNIT_TEST)
        self.time_table_mocked = mock.create_autospec(Timetable)
        self.uow_dao_mocked = mock.create_autospec(UnitOfWorkDao)
        self.job_dao_mocked = mock.create_autospec(JobDao)
        self.sm_real = AbstractStateMachine(self.logger, self.time_table_mocked, 'AbstractStateMachine')
        self.sm_real.uow_dao = self.uow_dao_mocked
        self.sm_real.job_dao = self.job_dao_mocked
        self.original_time_grouping = {process_name: context.process_context[process_name].time_grouping
                                       for process_name in [PROCESS_SITE_HOURLY, PROCESS_SITE_DAILY,
                                                            PROCESS_SITE_MONTHLY, PROCESS_SITE_YEARLY]
                                       }

    def tearDown(self):
        for process_name, time_grouping in self.original_time_grouping.items():
            context.process_context[process_name].time_grouping = time_grouping

    def test_insert_and_publish_uow(self):
        """ method tests happy-flow for insert_and_publish_uow method """
        self.sm_real._insert_uow = lambda *args: mock_insert_uow_return_uow(*args)
        self.sm_real._publish_uow = mock.MagicMock(return_value=True)

        job_record = Job(process_name=PROCESS_SITE_HOURLY, timeperiod=TEST_PRESET_TIMEPERIOD)
        manual_uow, _ = then_return_uow(job_record, 0, 1)
        uow, is_duplicate = self.sm_real.insert_and_publish_uow(job_record, 0, 1)

        self.assertFalse(is_duplicate)
        self.assertDictEqual(uow.document, manual_uow.document)

    def test_unhandled_exception_iapu(self):
        """ method tests unhandled UserWarning exception at insert_and_publish_uow method """
        self.sm_real._insert_uow = then_raise_uw
        self.sm_real._publish_uow = mock.MagicMock(return_value=True)

        try:
            job_record = Job(process_name=PROCESS_SITE_HOURLY, timeperiod=TEST_PRESET_TIMEPERIOD)
            self.sm_real.insert_and_publish_uow(job_record, 0, 1)
            self.assertTrue(False, 'UserWarning exception should have been thrown')
        except UserWarning:
            self.assertTrue(True)

    def test_handled_exception_iapu(self):
        """ method tests handled UserWarning exception at insert_and_publish_uow method """
        job_record = Job(process_name=PROCESS_SITE_HOURLY, timeperiod=TEST_PRESET_TIMEPERIOD)
        manual_uow, _ = then_return_uow(job_record, 0, 1)

        self.sm_real._insert_uow = mock_insert_uow_raise_dpk
        self.sm_real._publish_uow = mock.MagicMock(return_value=True)

        self.uow_dao_mocked.recover_from_duplicatekeyerror = mock.MagicMock(return_value=manual_uow)
        uow, is_duplicate = self.sm_real.insert_and_publish_uow(job_record, 0, 1)
        self.assertTrue(is_duplicate)
        self.assertDictEqual(uow.document, manual_uow.document)

    def test_noop_timeperiod(self):
        """ method tests how the time_grouping effects the job flow and whether job are transferred
            to the STATE_NOOP state for timeperiods that falls in-between grouping """
        self.sm_real._process_noop_timeperiod = mock.Mock(
            side_effect=self.sm_real._process_noop_timeperiod)
        self.sm_real._process_state_embryo = mock.Mock(
            side_effect=self.sm_real._process_state_embryo)

        process_hierarchy = mock.MagicMock()
        tree = mock.PropertyMock()
        type(tree).process_hierarchy = process_hierarchy
        self.time_table_mocked.get_tree = mock.MagicMock(side_effect=lambda _: tree)

        # case 1: job falls in-between time grouping
        context.process_context[PROCESS_SITE_HOURLY].time_grouping = 3
        type(process_hierarchy[PROCESS_SITE_HOURLY]).timeperiod_dict = \
            mock.PropertyMock(return_value=TimeperiodDict(QUALIFIER_HOURLY, 3))
        job_record = get_job_record(job.STATE_EMBRYO, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)

        self.sm_real.manage_job(job_record)
        self.assertEqual(len(self.job_dao_mocked.update.call_args_list), 1)
        self.assertEqual(len(self.sm_real._process_noop_timeperiod.call_args_list), 1)
        self.assertEqual(len(self.sm_real._process_state_embryo.call_args_list), 0)

        # reset call count
        self.sm_real._process_noop_timeperiod.reset_mock()
        self.sm_real._process_state_embryo.reset_mock()

        # case 2: there is no time grouping
        context.process_context[PROCESS_SITE_HOURLY].time_grouping = 1
        type(process_hierarchy[PROCESS_SITE_HOURLY]).timeperiod_dict = \
            mock.PropertyMock(return_value=TimeperiodDict(QUALIFIER_HOURLY, 1))
        job_record = get_job_record(job.STATE_EMBRYO, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)

        self.sm_real.manage_job(job_record)
        self.assertEqual(len(self.sm_real._process_noop_timeperiod.call_args_list), 0)
        self.assertEqual(len(self.sm_real._process_state_embryo.call_args_list), 1)

        # reset call count
        self.sm_real._process_noop_timeperiod.reset_mock()
        self.sm_real._process_state_embryo.reset_mock()

        # case 3: process has time_grouping and job falls on a grouped timeperiod
        context.process_context[PROCESS_SITE_HOURLY].time_grouping = 2
        type(process_hierarchy[PROCESS_SITE_HOURLY]).timeperiod_dict = \
            mock.PropertyMock(return_value=TimeperiodDict(QUALIFIER_HOURLY, 2))
        job_record = get_job_record(job.STATE_EMBRYO, TEST_PRESET_TIMEPERIOD, PROCESS_SITE_HOURLY)

        self.sm_real.manage_job(job_record)
        self.assertEqual(len(self.sm_real._process_noop_timeperiod.call_args_list), 0)
        self.assertEqual(len(self.sm_real._process_state_embryo.call_args_list), 1)

        # reset call count
        self.sm_real._process_noop_timeperiod.reset_mock()
        self.sm_real._process_state_embryo.reset_mock()

    def test_compute_start_timeperiod(self):
        fixture = {
            PROCESS_SITE_HOURLY: {'2010123123': '2010123122',
                                  '2010123122': '2010123122',
                                  '2010123112': '2010123110',
                                  '2010123111': '2010123110',
                                  '2010123101': '2010123100',
                                  '2010123100': '2010123100'},
            PROCESS_SITE_DAILY: {'2010123100': '2010123100',
                                 '2010120100': '2010120100',
                                 '2010120600': '2010120400',
                                 '2010123000': '2010122800',
                                 '2010122000': '2010121900'},
            PROCESS_SITE_MONTHLY: {'2010120000': '2010100000',
                                   '2010010000': '2010010000',
                                   '2010100000': '2010100000',
                                   '2010110000': '2010100000'},
        }

        process_hierarchy = mock.MagicMock()
        tree = mock.PropertyMock()
        type(tree).process_hierarchy = process_hierarchy
        self.time_table_mocked.get_tree = mock.MagicMock(side_effect=lambda _: tree)

        for process_name, f in fixture.items():
            context.process_context[process_name].time_grouping = 3
            type(process_hierarchy[process_name]).timeperiod_dict = \
                mock.PropertyMock(return_value=TimeperiodDict(context.process_context[process_name].time_qualifier,
                                                              context.process_context[process_name].time_grouping))

            for key, value in f.items():
                actual_value = self.sm_real.compute_start_timeperiod(process_name, key)
                self.assertEqual(actual_value, value,
                                 msg='failing combination: q={0} fixture={1}/{2} actual={3}'
                                 .format(process_name, key, value, actual_value))

        fixture = {
            PROCESS_SITE_YEARLY: {'2010000000': '2010000000',
                                  '2011000000': '2011000000'}
        }
        for process_name, f in fixture.items():
            try:
                context.process_context[process_name].time_grouping = 3
                for key, value in f.items():
                    print('*** Problematic set: {0} {1}->{2}'
                          .format(process_name, key, value))
                    _ = self.sm_real.compute_start_timeperiod(process_name, key)
                self.assertTrue(False, 'YEARLY should allow only identity grouping (i.e. grouping=1)')
            except AssertionError:
                self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
