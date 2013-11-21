__author__ = 'Bohdan Mushkevych'

import unittest
from base_fixtures import compare_dictionaries
from model import box_configuration_entry
from model.box_configuration_entry import BoxConfigurationEntry
from model.scheduler_configuration_entry import SchedulerConfigurationEntry
from model.time_table_entry import TimeTableEntry
from model.unit_of_work_entry import UnitOfWorkEntry


class TestSystemCollections(unittest.TestCase):
    
    def setUp(self):
        self.box_configuration = BoxConfigurationEntry()
        self.scheduler_configuration = SchedulerConfigurationEntry()
        self.timetable = TimeTableEntry()
        self.uow = UnitOfWorkEntry()

    def tearDown(self):
        del self.box_configuration
        del self.scheduler_configuration
        del self.timetable
        del self.uow

    def test_box_configuration(self):
        box_id = 'box_1'
        process_list = {
            'process_1' : {box_configuration_entry.STATE : box_configuration_entry.STATE_ON,
                           box_configuration_entry.PID : 1001},
            'process_2' : {box_configuration_entry.STATE : box_configuration_entry.STATE_ON,
                           box_configuration_entry.PID : 1001},
            'process_3' : {box_configuration_entry.STATE : box_configuration_entry.STATE_ON,
                           box_configuration_entry.PID : 1001},
        }
        self.box_configuration.box_id = box_id
        self.box_configuration.process_list = process_list
        self.box_configuration.process_state = ('process_2', box_configuration_entry.STATE_OFF)

        assert box_id == self.box_configuration.box_id
        compare_dictionaries(self.box_configuration.process_list, process_list)


if __name__ == '__main__':
    unittest.main()
