__author__ = 'Bohdan Mushkevych'

import unittest
from db.model.single_session import SingleSession


class TestSingleSession(unittest.TestCase):
    def setUp(self):
        self.obj = SingleSession()

    def tearDown(self):
        del self.obj

    def test_key(self):
        domain_name = 'test_name'
        timeperiod = '20110101161633'
        session_id = 'session_xxx_yyy'
        self.obj.key = (domain_name, timeperiod, session_id)
        temp = self.obj.key
        assert temp[0] == domain_name
        assert temp[1] == timeperiod
        assert temp[2] == session_id

    def test_session_id(self):
        value = 'value_1234567890'
        self.obj.session_id = value
        assert self.obj.session_id == value

    def test_os(self):
        value = 'Windows MS PS 7.0.0.0.1.1.2'
        self.obj.user_profile.os = value
        assert self.obj.user_profile.os == value

    def test_browser(self):
        value = 'FF 3.4.5.6.7.8.9'
        self.obj.user_profile.browser = value
        assert self.obj.user_profile.browser == value

    def test_ip(self):
        value = '100.100.200.200'
        self.obj.user_profile.ip = value
        assert self.obj.user_profile.ip == value

    def test_screen_res(self):
        value_x = 1080
        value_y = 980
        self.obj.user_profile.screen_res = (value_x, value_y)
        assert self.obj.user_profile.screen_x == value_x
        assert self.obj.user_profile.screen_y == value_y

    def test_language(self):
        value = 'ca-uk'
        self.obj.user_profile.language = value
        assert self.obj.user_profile.language == value

    def test_country(self):
        value = 'ca'
        self.obj.user_profile.country = value
        assert self.obj.user_profile.country == value

    def test_total_duration(self):
        value = 123
        self.obj.browsing_history.total_duration = value
        assert self.obj.browsing_history.total_duration == value

    def test_number_of_entries(self):
        value = 12
        self.obj.browsing_history.number_of_entries = value
        assert self.obj.browsing_history.number_of_entries == value


if __name__ == '__main__':
    unittest.main()
