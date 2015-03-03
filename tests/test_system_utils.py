# coding=utf-8
__author__ = 'Bohdan Mushkevych'

import unittest

from synergy.system.utils import unicode_truncate


class TestSystemUtils(unittest.TestCase):
    def test_unicode_truncation(self):
        fixture = dict()
        fixture[u"абвгдеєжзиіїйклмнопрстуфхцчшщюяь"] = u"абвгдеєжзи"
        fixture[u"ウィキペディアにようこそ"] = u"ウィキペディ"
        fixture["abcdefghijklmnopqrstuvwxyz"] = "abcdefghijklmnopqrst"
        fixture["Jan 06, 2010"] = "Jan 06, 2010"

        for k, v in fixture.items():
            actual = unicode_truncate(k, 20)
            self.assertTrue(actual == v, 'actual vs expected: %s vs %s' % (actual, v))


if __name__ == '__main__':
    unittest.main()
