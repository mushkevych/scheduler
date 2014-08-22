# coding=utf-8
__author__ = 'Bohdan Mushkevych'

import unittest
from tests.base_fixtures import compare_dictionaries
from system.utils import unicode_truncate
from db.model import box_configuration
from db.model.box_configuration import BoxConfiguration
from db.model.scheduler_entry import SchedulerEntry
from db.model.time_table_record import TimeTableRecord
from db.model.unit_of_work import UnitOfWork


class TestSystemUtils(unittest.TestCase):
    def test_unicode_truncation(self):
        fixture = dict()
        fixture[u"абвгдеєжзиіїйклмнопрстуфхцчшщюяь"] = u"абвгдеєжзи"
        fixture[u"ウィキペディアにようこそ"] = u"ウィキペディ"
        fixture["abcdefghijklmnopqrstuvwxyz"] = "abcdefghijklmnopqrst"
        fixture["Jan 06, 2010"] = "Jan 06, 2010"

        for k, v in fixture.iteritems():
            actual = unicode_truncate(k, 20)
            self.assertTrue(actual == v, 'actual vs expected: %s vs %s' % (actual, v))


if __name__ == '__main__':
    unittest.main()
