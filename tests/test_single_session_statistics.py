"""
Created on 2011-01-28

@author: Bohdan Mushkevych
"""

import unittest
from data_collections.single_session import SingleSessionStatistics  

class TestSingleSessionStatistics(unittest.TestCase):
    
    def setUp(self):
        self.obj = SingleSessionStatistics()
         
    def tearDown(self):
        del self.obj

    def test_key(self):
        domain_name = 'test_name'
        correct_timestamp = '20110101_161633'
        self.obj.composite_key(domain_name, correct_timestamp)
        temp = self.obj.get_key()
        assert temp[0] == domain_name
        assert temp[1] == correct_timestamp
        
    def test_session_id(self):
        value = 'value_1234567890'
        self.obj.set_session_id(value)
        temp = self.obj.get_session_id()
        assert temp == value

    def test_os(self):
        value = 'Windows MS PS 7.0.0.0.1.1.2'
        self.obj.set_ip(value)
        temp = self.obj.get_ip()
        assert temp == value

    def test_browser(self):
        value = 'FF 3.4.5.6.7.8.9'
        self.obj.set_ip(value)
        temp = self.obj.get_ip()
        assert temp == value

    def test_ip(self):
        value = '100.100.200.200'
        self.obj.set_ip(value)
        temp = self.obj.get_ip()
        assert temp == value

    def test_screen_res(self):
        value_x = 1080
        value_y = 980
        self.obj.set_screen_res(value_x, value_y)
        temp = self.obj.get_screen_res()
        assert temp[0] == value_x
        assert temp[1] == value_y

    def test_language(self):
        value = 'ca-uk'
        self.obj.set_language(value)
        temp = self.obj.get_language()
        assert temp == value
        
    def test_country(self):
        value = 'ca'
        self.obj.set_country(value)
        temp = self.obj.get_country()
        assert temp == value
    
    def test_total_duration(self):
        value = 123
        self.obj.set_total_duration(value)
        temp = self.obj.get_total_duration()
        assert temp == value

    def test_number_of_entries(self):
        value = 12
        self.obj.set_number_of_entries(value)
        temp = self.obj.get_number_of_entries()
        assert temp == value

        
if __name__ == '__main__':
    unittest.main()
