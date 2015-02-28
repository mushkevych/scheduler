__author__ = 'Bohdan Mushkevych'

import unittest
from tests.base_fixtures import compare_dictionaries
from synergy.db.model import box_configuration
from synergy.db.model.box_configuration import BoxConfiguration
from synergy.db.model.managed_process_entry import ManagedProcessEntry
from synergy.db.model.job import Job
from synergy.db.model.unit_of_work import UnitOfWork


class TestSystemCollections(unittest.TestCase):
    def setUp(self):
        self.box_configuration = BoxConfiguration()
        self.managed_entry = ManagedProcessEntry()
        self.job_record = Job()
        self.uow = UnitOfWork()

    def tearDown(self):
        del self.box_configuration
        del self.managed_entry
        del self.job_record
        del self.uow

    def test_box_configuration(self):
        box_id = 'box_1'
        process_list = {
            'process_1': {box_configuration.STATE: box_configuration.STATE_ON,
                          box_configuration.PID: 1001},
            'process_2': {box_configuration.STATE: box_configuration.STATE_ON,
                          box_configuration.PID: 1001},
            'process_3': {box_configuration.STATE: box_configuration.STATE_ON,
                          box_configuration.PID: 1001},
        }
        self.box_configuration.box_id = box_id
        self.box_configuration.process_list = process_list
        self.box_configuration.set_process_state('process_2', box_configuration.STATE_OFF)

        self.assertEqual(box_id, self.box_configuration.box_id)
        try:
            compare_dictionaries(self.box_configuration.process_list, process_list)
            self.assertTrue(False, 'process_2 state should cause process lists to be different')
        except:
            self.assertTrue(True, 'expected mismatch')

        self.box_configuration.set_process_state('process_2', box_configuration.STATE_ON)
        compare_dictionaries(self.box_configuration.process_list, process_list)


if __name__ == '__main__':
    unittest.main()
