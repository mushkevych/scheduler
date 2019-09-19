__author__ = 'Bohdan Mushkevych'

import unittest
from synergy.db.model.freerun_process_entry import build_schedulable_name, split_schedulable_name


class TestFreerunProcessEntry(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_build_schedulable_name(self):
        fixture = {
            ('prefix', 'suffix'): 'prefix::suffix',
            (None, 'suffix'): 'None::suffix',
            (None, None): 'None::None',
            ('', ''): '::',
            ('tratata', 'dududu::uuuu'): 'tratata::dududu::uuuu'
        }

        for k, v in fixture.items():
            actual = build_schedulable_name(*k)
            self.assertEqual(actual, v, f'actual vs expected: {actual} vs {v}')

    def test_split_schedulable_name(self):
        fixture = {
            'prefix::suffix': ('prefix', 'suffix'),
            'None::suffix': ('None', 'suffix'),
            'None::None': ('None', 'None'),
            '::': ('', ''),
            'tratata::dududu::uuuu': ('tratata', 'dududu::uuuu')
        }

        for k, v in fixture.items():
            actual = split_schedulable_name(k)
            self.assertSequenceEqual(actual, v, f'actual vs expected: {actual} vs {v}')


if __name__ == '__main__':
    unittest.main()
