# coding=utf-8
__author__ = 'Bohdan Mushkevych'

import unittest
from datetime import datetime, timedelta

import mock
from settings import enable_test_mode

enable_test_mode()

from synergy.db.model import unit_of_work
from synergy.db.model.unit_of_work import UnitOfWork
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao

from tests.base_fixtures import create_unit_of_work, create_and_insert_unit_of_work
from constants import *
from synergy.mq.flopsy import PublishersPool, Publisher
from synergy.scheduler.tree import LIFE_SUPPORT_HOURS
from synergy.scheduler.synergy_scheduler import Scheduler
from synergy.scheduler.thread_handler import ThreadHandler
from synergy.scheduler.garbage_collector import GarbageCollector, GarbageCollectorEntry
from synergy.system.data_logging import get_logger
from tests.ut_context import *


def get_invalid_and_fresh_uow():
    return create_unit_of_work(PROCESS_SITE_HOURLY,
                               0,
                               1,
                               state=unit_of_work.STATE_INVALID,
                               created_at=datetime.utcnow(),
                               uow_id=0)


def get_invalid_and_stale_uow():
    return create_unit_of_work(PROCESS_SITE_HOURLY,
                               0,
                               1,
                               state=unit_of_work.STATE_INVALID,
                               created_at=datetime.utcnow() - timedelta(hours=LIFE_SUPPORT_HOURS),
                               uow_id=0)


def get_valid_and_fresh_uow():
    return create_unit_of_work(PROCESS_SITE_HOURLY,
                               0,
                               1,
                               state=unit_of_work.STATE_IN_PROGRESS,
                               created_at=datetime.utcnow(),
                               uow_id=0)


def get_valid_and_stale_uow():
    return create_unit_of_work(PROCESS_SITE_HOURLY,
                               0,
                               1,
                               state=unit_of_work.STATE_REQUESTED,
                               created_at=datetime.utcnow() - timedelta(hours=LIFE_SUPPORT_HOURS),
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

        self.managed_handlers_mocked = dict()
        self.managed_handlers_mocked[PROCESS_SITE_HOURLY] = mock.create_autospec(ThreadHandler)
        self.scheduler_mocked.managed_handlers = self.managed_handlers_mocked

        self.publisher = mock.create_autospec(Publisher)
        self.scheduler_mocked.publishers = mock.create_autospec(PublishersPool)
        self.scheduler_mocked.publishers.get = mock.MagicMock(return_value=self.publisher)

        self.worker = GarbageCollector(self.scheduler_mocked)
        self.worker._resubmit_uow = mock.Mock(side_effect=self.worker._resubmit_uow)

    def tearDown(self):
        # killing the worker
        del self.worker

    def test_priority_queue_eq(self):
        uow = get_valid_and_fresh_uow()
        gce = GarbageCollectorEntry(uow)

        uow_2 = get_valid_and_fresh_uow()
        gce_2 = GarbageCollectorEntry(uow_2)

        uow_3 = get_valid_and_fresh_uow()
        uow_3.start_id += 'ttt'
        gce_3 = GarbageCollectorEntry(uow_3)

        q = self.worker.reprocess_uows[PROCESS_SITE_HOURLY]
        q.put_nowait(gce)

        self.assertEqual(gce, gce_2)
        self.assertNotEqual(gce, gce_3)
        self.assertIn(gce, self.worker.reprocess_uows)
        self.assertIn(gce_2, self.worker.reprocess_uows)
        self.assertNotIn(gce_3, self.worker.reprocess_uows)

    def test_invalid_and_fresh_uow(self):
        self.worker.uow_dao.update = assume_uow_is_requested
        self.worker._process_single_document(get_invalid_and_fresh_uow())
        self.publisher.publish.assert_called_once_with(mock.ANY)

    def test_invalid_and_stale_uow(self):
        self.worker.uow_dao.update = assume_uow_is_cancelled
        self.worker._process_single_document(get_invalid_and_stale_uow())
        # transferring job to STATE_CANCELED and performing optional MQ update
        self.publisher.publish.assert_called_once_with(mock.ANY)

    def test_valid_and_fresh_uow(self):
        self.worker.uow_dao.update = assume_uow_is_requested
        self.worker._process_single_document(get_valid_and_fresh_uow())
        self.assertTrue(self.publisher.publish.call_args_list == [])  # called 0 times

    def test_valid_and_stale_uow(self):
        self.worker.uow_dao.update = assume_uow_is_cancelled
        self.worker._process_single_document(get_valid_and_stale_uow())
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

        positive_timeperiods = {u'2010123123': PROCESS_SITE_HOURLY,    # hourly time qualifier
                                u'2010123100': PROCESS_SITE_DAILY,     # daily time qualifier
                                u'2010120000': PROCESS_SITE_MONTHLY,   # monthly time qualifier
                                u'2010000000': PROCESS_SITE_YEARLY}    # yearly time qualifier

        negative_timeperiods = {u'2009123123': PROCESS_SITE_HOURLY,    # hourly time qualifier
                                u'2009123100': PROCESS_SITE_DAILY,     # daily time qualifier
                                u'2009120000': PROCESS_SITE_MONTHLY,   # monthly time qualifier
                                u'2009000000': PROCESS_SITE_YEARLY}    # yearly time qualifier

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
