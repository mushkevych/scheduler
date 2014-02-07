__author__ = 'Bohdan Mushkevych'

import unittest
from datetime import datetime, timedelta
from mockito import spy, verify, mock, when
from mockito.matchers import any

from db.model import unit_of_work
from mq.flopsy import PublishersPool
from tests.base_fixtures import create_unit_of_work
from system.process_context import PROCESS_GC, PROCESS_UNIT_TEST
from workers.garbage_collector_worker import GarbageCollectorWorker, LIFE_SUPPORT_HOURS


def get_invalid_and_fresh_uow():
    return create_unit_of_work(PROCESS_UNIT_TEST,
                               0,
                               1,
                               state=unit_of_work.STATE_INVALID,
                               creation_at=datetime.utcnow(),
                               uow_id=0)


def get_invalid_and_stale_uow():
    return create_unit_of_work(PROCESS_UNIT_TEST,
                               0,
                               1,
                               state=unit_of_work.STATE_INVALID,
                               creation_at=datetime.utcnow() - timedelta(hours=LIFE_SUPPORT_HOURS),
                               uow_id=0)


def get_valid_and_fresh_uow():
    return create_unit_of_work(PROCESS_UNIT_TEST,
                               0,
                               1,
                               state=unit_of_work.STATE_IN_PROGRESS,
                               creation_at=datetime.utcnow(),
                               uow_id=0)


def get_valid_and_stale_uow():
    return create_unit_of_work(PROCESS_UNIT_TEST,
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
    def setUp(self):
        self.worker = GarbageCollectorWorker(PROCESS_GC)
        self.worker.publishers = mock(PublishersPool)
        self.publisher = mock()
        when(self.worker.publishers).get_publisher(any(str)).thenReturn(self.publisher)

    def tearDown(self):
        # killing the worker
        self.worker.__del__()
        del self.worker

    def test_invalid_and_fresh_uow(self):
        self.worker.uow_dao.update = assume_uow_is_requested
        spy_worker = spy(self.worker)
        spy_worker._process_single_document(get_invalid_and_fresh_uow())
        verify(self.publisher, times=1).publish(any(str))

    def test_invalid_and_stale_uow(self):
        self.worker.uow_dao.update = assume_uow_is_cancelled
        spy_worker = spy(self.worker)
        spy_worker._process_single_document(get_invalid_and_stale_uow())
        verify(self.publisher, times=0).publish(any(str))

    def test_valid_and_fresh_uow(self):
        self.worker.uow_dao.update = assume_uow_is_requested
        spy_worker = spy(self.worker)
        spy_worker._process_single_document(get_valid_and_fresh_uow())
        verify(self.publisher, times=0).publish(any(str))

    def test_valid_and_stale_uow(self):
        self.worker.uow_dao.update = assume_uow_is_cancelled
        spy_worker = spy(self.worker)
        spy_worker._process_single_document(get_valid_and_stale_uow())
        verify(self.publisher, times=0).publish(any(str))


if __name__ == '__main__':
    unittest.main()
