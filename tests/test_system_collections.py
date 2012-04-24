"""
Created on 2011-07-05

@author: Bohdan Mushkevych
"""

import unittest
from base_fixtures import compare_dictionaries
from supervisor.box_configuration_entry import BoxConfigurationEntry
from scheduler.scheduler_configuration_entry import SchedulerConfigurationEntry
from scheduler.time_table_entry import TimeTableEntry
from scheduler.unit_of_work_entry import UnitOfWorkEntry

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
            'process_1' : {BoxConfigurationEntry.STATE : BoxConfigurationEntry.STATE_ON,
                           BoxConfigurationEntry.PID : 1001},
            'process_2' : {BoxConfigurationEntry.STATE : BoxConfigurationEntry.STATE_ON,
                           BoxConfigurationEntry.PID : 1001},
            'process_3' : {BoxConfigurationEntry.STATE : BoxConfigurationEntry.STATE_ON,
                           BoxConfigurationEntry.PID : 1001},
        }
        self.box_configuration.set_box_id(box_id)
        self.box_configuration.set_process_list(process_list)
        self.box_configuration.set_process_state('process_2', BoxConfigurationEntry.STATE_OFF)

        assert box_id == self.box_configuration.get_box_id()
        compare_dictionaries(self.box_configuration.get_process_list(), process_list)


if __name__ == '__main__':
    unittest.main()
