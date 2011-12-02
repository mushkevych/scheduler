"""
Created on 2011-07-05

@author: Bohdan Mushkevych
"""

import unittest
from base_fixtures import compare_dictionaries
from supervisor.box_configuration_collection import BoxConfigurationCollection
from scheduler.scheduler_configuration_collection import SchedulerConfigurationCollection
from scheduler.time_table_collection import TimeTableCollection
from scheduler.units_of_work_collection import UnitsOfWorkCollection

class TestSystemCollections(unittest.TestCase):
    
    def setUp(self):
        self.box_configuration = BoxConfigurationCollection()
        self.scheduler_configuration = SchedulerConfigurationCollection()
        self.timetable = TimeTableCollection()
        self.uow = UnitsOfWorkCollection()

    def tearDown(self):
        del self.box_configuration
        del self.scheduler_configuration
        del self.timetable
        del self.uow

    def test_box_configuration(self):
        box_id = 'box_1'
        process_list = {
            'process_1' : {BoxConfigurationCollection.STATE : BoxConfigurationCollection.STATE_ON,
                           BoxConfigurationCollection.PID : 1001},
            'process_2' : {BoxConfigurationCollection.STATE : BoxConfigurationCollection.STATE_ON,
                           BoxConfigurationCollection.PID : 1001},
            'process_3' : {BoxConfigurationCollection.STATE : BoxConfigurationCollection.STATE_ON,
                           BoxConfigurationCollection.PID : 1001},
        }
        self.box_configuration.set_box_id(box_id)
        self.box_configuration.set_process_list(process_list)
        self.box_configuration.set_process_state('process_2', BoxConfigurationCollection.STATE_OFF)

        assert box_id == self.box_configuration.get_box_id()
        compare_dictionaries(self.box_configuration.get_process_list(), process_list)


if __name__ == '__main__':
    unittest.main()
