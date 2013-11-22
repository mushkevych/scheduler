__author__ = 'Bohdan Mushkevych'

import unittest
from model.single_session import SingleSessionStatistics


class TestSingleSessionStatistics(unittest.TestCase):
    def setUp(self):
        self.obj = SingleSessionStatistics()

    def tearDown(self):
        del self.obj

    def test_key(self):
        domain_name = 'test_name'
        correct_timestamp = '20110101_161633'
        self.obj.key = (domain_name, correct_timestamp)
        temp = self.obj.key
        assert temp[0] == domain_name
        assert temp[1] == correct_timestamp

    def test_session_id(self):
        value = 'value_1234567890'
        self.obj.session_id = value
        temp = self.obj.session_id
        assert temp == value

    def test_os(self):
        value = 'Windows MS PS 7.0.0.0.1.1.2'
        self.obj.ip = value
        temp = self.obj.ip
        assert temp == value

    def test_browser(self):
        value = 'FF 3.4.5.6.7.8.9'
        self.obj.ip = value
        temp = self.obj.ip
        assert temp == value

    def test_ip(self):
        value = '100.100.200.200'
        self.obj.ip = value
        temp = self.obj.ip
        assert temp == value

    def test_screen_res(self):
        value_x = 1080
        value_y = 980
        self.obj.screen_res = (value_x, value_y)
        temp = self.obj.screen_res
        assert temp[0] == value_x
        assert temp[1] == value_y

    def test_language(self):
        value = 'ca-uk'
        self.obj.language = value
        temp = self.obj.language
        assert temp == value

    def test_country(self):
        value = 'ca'
        self.obj.country = value
        temp = self.obj.country
        assert temp == value

    def test_total_duration(self):
        value = 123
        self.obj.total_duration = value
        temp = self.obj.total_duration
        assert temp == value

    def test_number_of_entries(self):
        value = 12
        self.obj.number_of_entries = value
        temp = self.obj.number_of_entries
        assert temp == value


if __name__ == '__main__':
    unittest.main()
