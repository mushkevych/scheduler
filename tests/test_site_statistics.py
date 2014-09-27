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
        key = self.obj.key
        assert key[0] == domain_name
        assert key[1] == correct_timestamp

    def test_os(self):
        key_1 = 'Windows MS PS 7.0.0.0.1.1.2'
        value_1 = 44
        key_2 = 'Windows MS XP SP 3'
        value_2 = 11

        doc = self.obj.os
        doc[key_1] = value_1
        doc[key_2] = value_2
        self.obj.os = doc

        doc = self.obj.os
        assert doc[key_1] == value_1
        assert doc[key_2] == value_2

    def test_browsers(self):
        key_1 = 'FireFox 2.3.4.5.6'
        value_1 = 22
        key_2 = 'MS IE 9.9.9.9.9'
        value_2 = 11

        doc = self.obj.browsers
        doc[key_1] = value_1
        doc[key_2] = value_2
        self.obj.browsers = doc

        doc = self.obj.browsers
        assert doc[key_1] == value_1
        assert doc[key_2] == value_2

    def test_number_of_visits(self):
        value = 100
        self.obj.number_of_visits = value
        t = self.obj.number_of_visits
        assert t == value

    def test_total_duration(self):
        value = 100
        self.obj.total_duration = value
        t = self.obj.total_duration
        assert t == value

    def test_screen_res(self):
        key_1 = (1024, 768)
        value_1 = 2000
        key_2 = (360, 240)
        value_2 = 98765

        doc = self.obj.screen_res
        doc[key_1] = value_1
        doc[key_2] = value_2
        self.obj.screen_res = doc

        doc = self.obj.screen_res
        assert doc[key_1] == value_1
        assert doc[key_2] == value_2

    def test_languages(self):
        key_1 = 'en_ca'
        value_1 = 87878
        key_2 = 'ua_uk'
        value_2 = 98765

        doc = self.obj.languages
        doc[key_1] = value_1
        doc[key_2] = value_2
        self.obj.languages = doc

        doc = self.obj.languages
        assert doc[key_1] == value_1
        assert doc[key_2] == value_2

    def test_countries(self):
        key_1 = 'ca'
        value_1 = 87878
        key_2 = 'uk'
        value_2 = 98765

        doc = self.obj.countries
        doc[key_1] = value_1
        doc[key_2] = value_2
        self.obj.countries = doc

        doc = self.obj.countries
        assert doc[key_1] == value_1
        assert doc[key_2] == value_2


if __name__ == '__main__':
    unittest.main()
