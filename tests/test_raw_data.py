__author__ = 'Bohdan Mushkevych'

import time
import datetime
import unittest
from db.model.raw_data import RawData


class TestRawData(unittest.TestCase):
    def setUp(self):
        self.obj = RawData()

    def tearDown(self):
        del self.obj

    def test_key(self):
        domain_name = 'test_name'
        timestamp = time.time()
        session_id = 'session_xxx_yyy'
        self.obj.key = (domain_name, timestamp, session_id)
        assert self.obj.key[0] == domain_name
        assert self.obj.key[1] == datetime.datetime.fromtimestamp(timestamp)
        assert self.obj.key[2] == session_id

    def test_session_id(self):
        value = 'value_1234567890'
        self.obj.session_id = value
        assert self.obj.session_id == value

    def test_os(self):
        value = 'Windows MS PS 7.0.0.0.1.1.2'
        self.obj.os = value
        assert self.obj.os == value

    def test_browser(self):
        br_type = 'FireFox'
        version = '3.4.5.6.7.8.9'
        self.obj.browser = br_type + version
        assert self.obj.browser == br_type + version

    def test_ip(self):
        value = '100.100.200.200'
        self.obj.ip = value
        assert self.obj.ip == value

    def test_screen_res(self):
        value_x = 1080
        value_y = 980
        self.obj.screen_res = (value_x, value_y)
        assert self.obj.screen_x == value_x
        assert self.obj.screen_y == value_y

    def test_language(self):
        value = 'ca-uk'
        self.obj.language = value
        assert self.obj.language == value

    def test_country(self):
        value = 'ca'
        self.obj.country = value
        assert self.obj.country == value


if __name__ == '__main__':
    unittest.main()
