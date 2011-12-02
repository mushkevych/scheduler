"""
Created on 2011-03-30

@author: Bohdan Mushkevych
"""

import unittest
import workers
from tests import hourly_fixtures
from system.process_context import PROCESS_GC
from workers.garbage_collector_worker import GarbageCollectorWorker


class GarbageCollectorUnitTest(unittest.TestCase):

    def setUp(self):
        hourly_fixtures.clean_session_entries()
        hourly_fixtures.generated_session_entries()
        workers.garbage_collector_worker.TIMER_INTERVAL = 10
        self.worker = GarbageCollectorWorker(PROCESS_GC)

    def tearDown(self):
        # cleaning up DB
        hourly_fixtures.clean_session_entries()

        # killing the worker
        self.worker.__del__()
        del self.worker

    def test_s3(self):
        self.worker._perform_EA('20010304')
        pass


if __name__ == '__main__':
    unittest.main()
