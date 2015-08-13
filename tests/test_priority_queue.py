# coding=utf-8

__author__ = 'Bohdan Mushkevych'

import unittest
from datetime import datetime

from constants import *
from synergy.db.model import unit_of_work
from synergy.system.priority_queue import PriorityQueue, EventEntry
from tests.base_fixtures import create_unit_of_work


def create_test_uow():
    return create_unit_of_work(PROCESS_SITE_HOURLY,
                               0,
                               1,
                               state=unit_of_work.STATE_IN_PROGRESS,
                               created_at=datetime.utcnow(),
                               uow_id=0)


class PriorityQueueUnitTest(unittest.TestCase):
    def setUp(self):
        self.priority_queue = PriorityQueue()

    def tearDown(self):
        # killing the worker
        del self.priority_queue

    def test_contains(self):
        uow = create_test_uow()
        gce = EventEntry(uow)

        uow_2 = create_test_uow()
        gce_2 = EventEntry(uow_2)

        uow_3 = create_test_uow()
        uow_3.start_id += 'ttt'
        gce_3 = EventEntry(uow_3)

        self.priority_queue.put(gce)

        self.assertEqual(gce, gce_2)
        self.assertNotEqual(gce, gce_3)
        self.assertIn(gce, self.priority_queue)
        self.assertIn(gce_2, self.priority_queue)
        self.assertNotIn(gce_3, self.priority_queue)

    def test_pop_min(self):
        uow_1 = create_test_uow()
        uow_1.start_id = '111'
        gce_1 = EventEntry(uow_1, lag_in_minutes=5)

        uow_2 = create_test_uow()
        uow_2.start_id = '222'
        gce_2 = EventEntry(uow_2, lag_in_minutes=1)

        uow_3 = create_test_uow()
        uow_3.start_id = '333'
        gce_3 = EventEntry(uow_3, lag_in_minutes=10)

        self.priority_queue.put(gce_1)
        self.priority_queue.put(gce_2)
        self.priority_queue.put(gce_3)

        self.assertEqual(gce_2, self.priority_queue.pop())
        self.assertEqual(gce_1, self.priority_queue.pop())
        self.assertEqual(gce_3, self.priority_queue.pop())

    def test_peek(self):
        uow_1 = create_test_uow()
        uow_1.start_id = '111'
        gce_1 = EventEntry(uow_1, lag_in_minutes=5)

        uow_2 = create_test_uow()
        uow_2.start_id = '222'
        gce_2 = EventEntry(uow_2, lag_in_minutes=1)

        uow_3 = create_test_uow()
        uow_3.start_id = '333'
        gce_3 = EventEntry(uow_3, lag_in_minutes=10)

        self.priority_queue.put(gce_1)
        self.priority_queue.put(gce_2)
        self.priority_queue.put(gce_3)

        # verify that the peek is not removing the min entry
        self.assertEqual(gce_2, self.priority_queue.peek())
        self.assertEqual(gce_2, self.priority_queue.peek())


if __name__ == '__main__':
    unittest.main()
