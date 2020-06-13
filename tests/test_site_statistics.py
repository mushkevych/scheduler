__author__ = 'Bohdan Mushkevych'

import unittest
from db.model.site_statistics import SiteStatistics


class TestSiteStatistics(unittest.TestCase):
    def setUp(self):
        self.obj = SiteStatistics()

    def tearDown(self):
        del self.obj

    def test_key(self):
        domain_name = 'test_name'
        correct_timestamp = '20110101'
        self.obj.key = (domain_name, correct_timestamp)
        assert self.obj.key[0] == domain_name
        assert self.obj.key[1] == correct_timestamp

    def test_os(self):
        key_1 = 'Windows MS PS 7.0.0.0.1.1.2'
        value_1 = 44
        key_2 = 'Windows MS XP SP 3'
        value_2 = 11

        doc = self.obj.stat.os
        doc[key_1] = value_1
        doc[key_2] = value_2

        assert self.obj.stat.os[key_1] == value_1
        assert self.obj.stat.os[key_2] == value_2

    def test_browsers(self):
        key_1 = 'FireFox 2.3.4.5.6'
        value_1 = 22
        key_2 = 'MS IE 9.9.9.9.9'
        value_2 = 11

        doc = self.obj.stat.browser
        doc[key_1] = value_1
        doc[key_2] = value_2

        assert self.obj.stat.browser[key_1] == value_1
        assert self.obj.stat.browser[key_2] == value_2

    def test_number_of_visits(self):
        value = 100
        self.obj.stat.number_of_visits = value
        assert self.obj.stat.number_of_visits == value

    def test_total_duration(self):
        value = 100
        self.obj.stat.total_duration = value
        assert self.obj.stat.total_duration == value

    def test_screen_res(self):
        key_1 = (1024, 768)
        value_1 = 2000
        key_2 = (360, 240)
        value_2 = 98765

        doc = self.obj.stat.screen_resolution
        doc[key_1] = value_1
        doc[key_2] = value_2

        assert self.obj.stat.screen_resolution[key_1] == value_1
        assert self.obj.stat.screen_resolution[key_2] == value_2

    def test_languages(self):
        key_1 = 'en_ca'
        value_1 = 87878
        key_2 = 'ua_uk'
        value_2 = 98765

        doc = self.obj.stat.language
        doc[key_1] = value_1
        doc[key_2] = value_2

        assert self.obj.stat.language[key_1] == value_1
        assert self.obj.stat.language[key_2] == value_2

    def test_countries(self):
        key_1 = 'ca'
        value_1 = 87878
        key_2 = 'uk'
        value_2 = 98765

        doc = self.obj.stat.country
        doc[key_1] = value_1
        doc[key_2] = value_2

        assert self.obj.stat.country[key_1] == value_1
        assert self.obj.stat.country[key_2] == value_2


if __name__ == '__main__':
    unittest.main()
