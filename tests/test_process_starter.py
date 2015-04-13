__author__ = 'Bohdan Mushkevych'

import mock
import types
import unittest

from settings import enable_test_mode
enable_test_mode()

import process_starter
from six import class_types, PY2, PY3


def main_function(*args):
    return args


class OldClass:
    def starter_method(self, *args):
        return args


class NewClass(object):
    def starter_method(self, *args):
        return args


class TestProcessStarter(unittest.TestCase):
    def test_type_old_class(self):
        t, m, starter = process_starter.get_class('tests.test_process_starter.OldClass')
        self.assertIn(t, class_types)
        self.assertIsInstance(m, class_types)
        self.assertIsNone(starter)

    def test_type_new_class(self):
        t, m, starter = process_starter.get_class('tests.test_process_starter.NewClass')
        self.assertIn(t, class_types)
        self.assertIsInstance(m, class_types)
        self.assertIsNone(starter)

    def test_type_function(self):
        t, m, starter = process_starter.get_class('tests.test_process_starter.main_function')
        self.assertEqual(t, types.FunctionType)
        self.assertIsInstance(m, types.FunctionType)
        self.assertIsNone(starter)

    def test_old_class_method(self):
        t, m, starter = process_starter.get_class('tests.test_process_starter.OldClass.starter_method')
        self.assertIn(t, class_types)
        self.assertIsInstance(m, class_types)
        self.assertEqual(starter, 'starter_method')

    def test_not_class(self):
        t, m, starter = process_starter.get_class('tests.test_process_starter.main_function')
        self.assertEqual(t, types.FunctionType)
        self.assertIsInstance(m, types.FunctionType)
        self.assertNotIsInstance(m, class_types)
        self.assertIsNone(starter)

    def test_starter_method(self):
        t, m, starter = process_starter.get_class('tests.test_process_starter.NewClass.starter_method')
        self.assertIn(t, class_types)
        self.assertIsInstance(m, class_types)
        self.assertEqual(starter, 'starter_method')

        self.assertIsInstance(getattr(m(), starter), types.MethodType)
        if PY2:
            self.assertIsInstance(getattr(m, starter), types.MethodType)
        if PY3:
            self.assertIsInstance(getattr(m, starter), types.FunctionType)

    @mock.patch('synergy.workers.abstract_mq_worker.SimpleTracker')
    @mock.patch('synergy.workers.abstract_mq_worker.Consumer')
    def test_starting_method(self, mock_tracker, mock_consumer):
        """
        performance_ticker and Flopsy consumer must be mocked
        otherwise they will instantiate threads
        and cause Unit Tests to fail to finish
        """
        from tests.ut_context import PROCESS_CLASS_EXAMPLE
        process_starter.start_by_process_name(PROCESS_CLASS_EXAMPLE, None)

    def test_starting_function(self):
        from tests.ut_context import PROCESS_SCRIPT_EXAMPLE
        process_starter.start_by_process_name(PROCESS_SCRIPT_EXAMPLE, 'parameters')


if __name__ == '__main__':
    unittest.main()
