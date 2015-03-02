# coding=utf-8
__author__ = 'Bohdan Mushkevych'

import unittest
from datetime import datetime, timedelta

from mockito import spy, verify, mock, when
from mockito.matchers import any

from settings import enable_test_mode
enable_test_mode()

from synergy.db.model import unit_of_work
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao

from tests.base_fixtures import create_unit_of_work, create_and_insert_unit_of_work
from constants import *
from synergy.mq.flopsy import PublishersPool
from synergy.scheduler.scheduler_constants import PROCESS_GC
from synergy.workers.garbage_collector_worker import GarbageCollectorWorker, LIFE_SUPPORT_HOURS
from synergy.system.data_logging import get_logger
from tests.ut_context import *


def get_invalid_and_fresh_uow():
    return create_unit_of_work(PROCESS_SITE_HOURLY,
                               0,
                               1,
                               state=unit_of_work.STATE_INVALID,
                               creation_at=datetime.utcnow(),
                               uow_id=0)


def get_invalid_and_stale_uow():
    return create_unit_of_work(PROCESS_SITE_HOURLY,
                               0,
                               1,
                               state=unit_of_work.STATE_INVALID,
                               creation_at=datetime.utcnow() - timedelta(hours=LIFE_SUPPORT_HOURS),
                               uow_id=0)


def get_valid_and_fresh_uow():
    return create_unit_of_work(PROCESS_SITE_HOURLY,
                               0,
                               1,
                               state=unit_of_work.STATE_IN_PROGRESS,
                               creation_at=datetime.utcnow(),
                               uow_id=0)


def get_valid_and_stale_uow():
    return create_unit_of_work(PROCESS_SITE_HOURLY,
                               0,
                               1,
                               state=unit_of_work.STATE_REQUESTED,
                               creation_at=datetime.utcnow() - timedelta(hours=LIFE_SUPPORT_HOURS),
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
        self.worker = GarbageCollectorWorker(PROCESS_GC)
        self.worker.publishers = mock(PublishersPool)
        self.publisher = mock()
        when(self.worker.publishers).get(any(str)).thenReturn(self.publisher)

    def tearDown(self):
        # killing the worker
        self.worker.performance_ticker.cancel()
        del self.worker

    def test_invalid_and_fresh_uow(self):
        self.worker.uow_dao.update = assume_uow_is_requested
        spy_worker = spy(self.worker)
        spy_worker._process_single_document(get_invalid_and_fresh_uow())
        verify(self.publisher, times=1).publish(any(dict))

    def test_invalid_and_stale_uow(self):
        self.worker.uow_dao.update = assume_uow_is_cancelled
        spy_worker = spy(self.worker)
        spy_worker._process_single_document(get_invalid_and_stale_uow())
        # transferring job to STATE_CANCELED and performing optional MQ update
        verify(self.publisher, times=1).publish(any(dict))

    def test_valid_and_fresh_uow(self):
        self.worker.uow_dao.update = assume_uow_is_requested
        spy_worker = spy(self.worker)
        spy_worker._process_single_document(get_valid_and_fresh_uow())
        verify(self.publisher, times=0).publish(any(dict))

    def test_valid_and_stale_uow(self):
        self.worker.uow_dao.update = assume_uow_is_cancelled
        spy_worker = spy(self.worker)
        spy_worker._process_single_document(get_valid_and_stale_uow())
        verify(self.publisher, times=1).publish(any(dict))

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
        for timeperiod, process_name in all_timeperiods.iteritems():
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
