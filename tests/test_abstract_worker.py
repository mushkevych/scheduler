__author__ = 'Bohdan Mushkevych'

import unittest
from db.model import unit_of_work_dao
from tests import base_fixtures
from tests.base_fixtures import TestMessage
from tests.base_fixtures import TestConsumer


# pylint: disable=E1002
def get_test_aggregator(baseclass, process_name):
    class TestAggregator(baseclass):
        def __init__(self, process_name):
            super(TestAggregator, self).__init__(process_name)
            self.consumer = TestConsumer()

        def _flush_aggregated_objects(self):
            pass

        def _flush_unallocated_objects(self, array):
            pass

    return TestAggregator(process_name)


# pylint: disable=E1101
class AbstractWorkerUnitTest(unittest.TestCase):
    def constructor(self, baseclass, process_name, output_prefix, output_module, generate_output, compare_results):
        self.baseclass = baseclass
        self.process_name = process_name
        self.output_prefix = output_prefix
        self.output_module = output_module
        self.generate_output = generate_output
        self.compare_results = compare_results

    def virtual_set_up(self):
        """Abstract method to be implemented by children"""
        pass

    def setUp(self):
        list_of_stats = self.virtual_set_up()
        self.aggregator = get_test_aggregator(self.baseclass, self.process_name)

        # creating unit_of_work entity, requesting to process created above statistics
        self.uow_id = base_fixtures.create_unit_of_work(self.process_name,
                                                        str(list_of_stats[0]),
                                                        str(list_of_stats[-1]))

    def virtual_tear_down(self):
        """Abstract method to be implemented by children"""
        pass

    def tearDown(self):
        # cleaning up DB
        self.virtual_tear_down()
        unit_of_work_dao.remove(self.aggregator.logger, self.uow_id)
        # killing the worker
        #        self.aggregator.__del__()
        del self.aggregator

    def _get_key(self, obj):
        """Abstract method to be implemented by children"""
        pass

    def perform_aggregation(self):
        message = TestMessage()
        message.body = str(self.uow_id)
        self.aggregator._mq_callback(message)

        if self.generate_output:
            i = 0
            for key in self.aggregator.aggregated_objects:
                print self.output_prefix + '_%02d = %s' % (i, self.aggregator.aggregated_objects[key].document)
                i += 1

        if self.compare_results:
            expected_values = base_fixtures.get_field_starting_with(self.output_prefix, self.output_module)
            if len(expected_values) == 0:
                assert 1 == 0
            for obj in expected_values:
                document = self.aggregator.aggregated_objects[self._get_key(obj)].document
                base_fixtures.compare_dictionaries(document, obj)


if __name__ == '__main__':
    unittest.main()
