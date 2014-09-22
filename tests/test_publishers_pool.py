__author__ = 'Bohdan Mushkevych'

import unittest

import mock

from mq.flopsy import PublishersPool, _Pool, Publisher
from conf.process_context import ProcessContext
from tests.ut_context import PROCESS_UNIT_TEST, register_processes


class TestPublishersPool(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestPublishersPool, cls).setUpClass()
        register_processes()

    def setUp(self):
        super(TestPublishersPool, self).setUp()
        self.logger = ProcessContext.get_logger(PROCESS_UNIT_TEST)

    def test_pop(self):
        """ test to prove that neither Publisher.__del__ nor Publisher.release are called on PublisherPool.get """
        publisher = Publisher(PROCESS_UNIT_TEST)
        mock_publisher = mock.Mock(wraps=publisher, spec=Publisher)

        single_pool = _Pool(logger=self.logger, name=PROCESS_UNIT_TEST)
        mock_single_pool = mock.Mock(wraps=single_pool, spec=_Pool)
        single_pool.publishers.append(mock_publisher)

        pools = PublishersPool(self.logger)
        mock_pools = mock.Mock(wraps=pools, spec=PublishersPool)
        pools.pools[PROCESS_UNIT_TEST] = mock_single_pool

        p = mock_pools.get(PROCESS_UNIT_TEST)
        self.assertEqual(p, mock_publisher)
        self.assertEqual(mock_pools.get.call_count, 1)
        self.assertEqual(mock_single_pool.get.call_count, 1)
        self.assertEqual(mock_publisher.release.call_count, 0)
        self.assertEqual(mock_publisher.close.call_count, 0)
        self.assertEqual(mock_publisher.__del__.call_count, 0)
        self.assertTrue(len(single_pool.publishers) == 0)


if __name__ == '__main__':
    unittest.main()
