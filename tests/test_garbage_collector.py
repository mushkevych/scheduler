# coding=utf-8
__author__ = 'Bohdan Mushkevych'

import unittest
from datetime import datetime, timedelta

import mock
from settings import enable_test_mode

enable_test_mode()

from synergy.conf import settings
from synergy.db.model import unit_of_work
from synergy.db.model.managed_process_entry import ManagedProcessEntry
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao

from tests.base_fixtures import create_unit_of_work, create_and_insert_unit_of_work
from constants import *
from synergy.mq.flopsy import PublishersPool, Publisher
from synergy.scheduler.tree import LIFE_SUPPORT_HOURS
from synergy.scheduler.timetable import Timetable
from synergy.scheduler.synergy_scheduler import Scheduler
from synergy.scheduler.thread_handler import ManagedThreadHandler
from synergy.scheduler.garbage_collector import GarbageCollector
from synergy.system.data_logging import get_logger
from tests.ut_context import *


def get_invalid_and_fresh_uow():
    return create_unit_of_work(
        PROCESS_SITE_HOURLY,
        0,
        1,
        state=unit_of_work.STATE_INVALID,
        created_at=datetime.utcnow(),
        submitted_at=datetime.utcnow(),
        uow_id=0)


def get_invalid_and_stale_uow():
    return create_unit_of_work(
        PROCESS_SITE_HOURLY,
        0,
        1,
        state=unit_of_work.STATE_INVALID,
        created_at=datetime.utcnow() - timedelta(hours=LIFE_SUPPORT_HOURS),
        submitted_at=datetime.utcnow() - timedelta(hours=settings.settings['gc_resubmit_after_hours']),
        uow_id=0)


def get_valid_and_fresh_uow():
    return create_unit_of_work(
        PROCESS_SITE_HOURLY,
        0,
        1,
        state=unit_of_work.STATE_IN_PROGRESS,
        created_at=datetime.utcnow(),
        submitted_at=datetime.utcnow(),
        uow_id=0)


def get_valid_and_stale_uow():
    return create_unit_of_work(
        PROCESS_SITE_HOURLY,
        0,
        1,
        state=unit_of_work.STATE_REQUESTED,
        created_at=datetime.utcnow() - timedelta(hours=LIFE_SUPPORT_HOURS),
        submitted_at=datetime.utcnow() - timedelta(hours=settings.settings['gc_resubmit_after_hours']),
        uow_id=0)


def assume_uow_is_cancelled(uow):
    assert unit_of_work.STATE_CANCELED == uow.state


def assume_uow_is_requested(uow):
    assert unit_of_work.STATE_REQUESTED == uow.state


class GarbageCollectorUnitTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(GarbageCollectorUnitTest, cls).setUpClass()

    def setUp(self):
        self.scheduler_mocked = mock.create_autospec(Scheduler)
        self.time_table_mocked = mock.create_autospec(Timetable)
        self.scheduler_mocked.timetable = self.time_table_mocked

        self.process_entry = mock.create_autospec(ManagedProcessEntry)
        self.process_entry.is_on = True

        self.thread_handler = mock.create_autospec(ManagedThreadHandler)
        self.thread_handler.process_entry = self.process_entry

        self.managed_handlers_mocked = dict()
        self.managed_handlers_mocked[PROCESS_SITE_HOURLY] = self.thread_handler
        self.scheduler_mocked.managed_handlers = self.managed_handlers_mocked

        self.publisher = mock.create_autospec(Publisher)
        self.scheduler_mocked.publishers = mock.create_autospec(PublishersPool)
        self.scheduler_mocked.publishers.get = mock.MagicMock(return_value=self.publisher)

        self.worker = GarbageCollector(self.scheduler_mocked)
        self.worker._resubmit_uow = mock.Mock(side_effect=self.worker._resubmit_uow)
        self.worker._cancel_uow = mock.Mock(side_effect=self.worker._cancel_uow)

    def tearDown(self):
        # killing the worker
        del self.worker

    def test_invalid_and_fresh_uow(self):
        uow = get_invalid_and_fresh_uow()
        self.worker.uow_dao.get_reprocessing_candidates = mock.MagicMock(return_value=[uow])
        self.assertEqual(len(self.worker.reprocess_uows[uow.process_name]), 0)

        # use-case 1 - UOW is invalid, and added to the reprocessing_queue
        self.worker.scan_uow_candidates()
        self.assertEqual(len(self.worker.reprocess_uows[uow.process_name]), 1)

        # use-case 2 - age the UOW by 2 hours. However, only 1 copy should be listed in the reprocessing_queue
        uow.submitted_at = datetime.utcnow() - timedelta(hours=2)
        self.worker.scan_uow_candidates()
        self.assertEqual(len(self.worker.reprocess_uows[uow.process_name]), 1)

        # use-case 3 - ignore the UOW, as it is already in the reprocessing_queue
        self.worker.scan_uow_candidates()
        self.assertEqual(len(self.worker.reprocess_uows[uow.process_name]), 1)

    def test_invalid_and_stale_uow(self):
        uow = get_invalid_and_stale_uow()
        self.worker.uow_dao.update = assume_uow_is_cancelled

        self.worker.uow_dao.get_reprocessing_candidates = mock.MagicMock(return_value=[uow])
        self.assertEqual(len(self.worker.reprocess_uows[uow.process_name]), 0)

        # use-case - transferring job to STATE_CANCELED bypassing the reprocessing_queue
        self.worker.scan_uow_candidates()
        self.assertEqual(len(self.worker.reprocess_uows[uow.process_name]), 0)
        self.worker._cancel_uow.assert_called_once_with(uow)
        self.publisher.publish.assert_called_once_with(mock.ANY)

    def test_valid_and_fresh_uow(self):
        uow = get_valid_and_fresh_uow()
        self.worker.uow_dao.update = assume_uow_is_requested

        self.worker.uow_dao.get_reprocessing_candidates = mock.MagicMock(return_value=[uow])
        self.assertEqual(len(self.worker.reprocess_uows[uow.process_name]), 0)

        # use-case - uow is healthy and should be filtered out by the DAO
        # it does not get queued for re-processing, because has not yet crossed 1 hour after submission
        self.worker.scan_uow_candidates()
        self.assertEqual(len(self.worker.reprocess_uows[uow.process_name]), 0)
        self.assertTrue(self.worker._cancel_uow.call_args_list == [])  # called 0 times
        self.assertTrue(self.publisher.publish.call_args_list == [])  # called 0 times

    def test_valid_and_stale_uow(self):
        uow = get_valid_and_stale_uow()
        self.worker.uow_dao.update = assume_uow_is_cancelled

        self.worker.uow_dao.get_reprocessing_candidates = mock.MagicMock(return_value=[uow])
        self.assertEqual(len(self.worker.reprocess_uows[uow.process_name]), 0)

        # use-case - transferring job to STATE_CANCELED bypassing the reprocessing_queue
        self.worker.scan_uow_candidates()
        self.assertEqual(len(self.worker.reprocess_uows[uow.process_name]), 0)
        self.worker._cancel_uow.assert_called_once_with(mock.ANY)
        self.publisher.publish.assert_called_once_with(mock.ANY)

    def test_select_reprocessing_candidates(self):
        logger = get_logger(PROCESS_UNIT_TEST)
        uow_dao = UnitOfWorkDao(logger)

        try:
            initial_candidates = uow_dao.get_reprocessing_candidates()
        except:
            initial_candidates = []

        try:
            initial_positive_candidates = uow_dao.get_reprocessing_candidates('2010123123')
        except:
            initial_positive_candidates = []

        positive_timeperiods = {u'2010123123': PROCESS_SITE_HOURLY,  # hourly time qualifier
                                u'2010123100': PROCESS_SITE_DAILY,  # daily time qualifier
                                u'2010120000': PROCESS_SITE_MONTHLY,  # monthly time qualifier
                                u'2010000000': PROCESS_SITE_YEARLY}  # yearly time qualifier

        negative_timeperiods = {u'2009123123': PROCESS_SITE_HOURLY,  # hourly time qualifier
                                u'2009123100': PROCESS_SITE_DAILY,  # daily time qualifier
                                u'2009120000': PROCESS_SITE_MONTHLY,  # monthly time qualifier
                                u'2009000000': PROCESS_SITE_YEARLY}  # yearly time qualifier

        all_timeperiods = dict()
        all_timeperiods.update(positive_timeperiods)
        all_timeperiods.update(negative_timeperiods)

        created_uow = []
        for timeperiod, process_name in all_timeperiods.items():
            created_uow.append(create_and_insert_unit_of_work(process_name,
                                                              0,
                                                              1,
                                                              timeperiod=timeperiod,
                                                              state=unit_of_work.STATE_INVALID))

        candidates = uow_dao.get_reprocessing_candidates('2010123123')
        self.assertEqual(len(candidates) - len(initial_positive_candidates), len(positive_timeperiods))

        candidates = uow_dao.get_reprocessing_candidates()
        self.assertEqual(len(candidates) - len(initial_candidates), len(all_timeperiods))

        for uow_id in created_uow:
            uow_dao.remove(uow_id)


if __name__ == '__main__':
    unittest.main()
