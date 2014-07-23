__author__ = 'Bohdan Mushkevych'

import types
import unittest
import process_starter


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
        assert isinstance(m, (type, types.ClassType))
        assert starter is None

    def test_type_new_class(self):
        t, m, starter = process_starter.get_class('tests.test_process_starter.NewClass')
        assert isinstance(m, (type, types.ClassType))
        assert starter is None

    def test_type_function(self):
        t, m, starter = process_starter.get_class('tests.test_process_starter.main_function')
        assert isinstance(m, (type, types.FunctionType))
        assert starter is None

    def test_old_class_method(self):
        t, m, starter = process_starter.get_class('tests.test_process_starter.OldClass.starter_method')
        assert isinstance(m, (type, types.ClassType))
        assert starter == 'starter_method'

    def test_not_class(self):
        t, m, starter = process_starter.get_class('tests.test_process_starter.main_function')
        assert not isinstance(m, (type, types.ClassType))
        assert starter is None

    def test_starter_method(self):
        t, m, starter = process_starter.get_class('tests.test_process_starter.NewClass.starter_method')
        assert isinstance(m, (type, types.ClassType))
        assert starter == 'starter_method'

        instance = getattr(m, starter)
        assert not isinstance(instance, (type, types.FunctionType))
        assert isinstance(instance, (type, types.MethodType))

    def test_starting_method(self):
        from tests.ut_process_context import PROCESS_CLASS_EXAMPLE
        process_starter.start_by_process_name(PROCESS_CLASS_EXAMPLE, None)

    def test_starting_function(self):
        from tests.ut_process_context import PROCESS_SCRIPT_EXAMPLE
        process_starter.start_by_process_name(PROCESS_SCRIPT_EXAMPLE, 'parameters')



if __name__ == '__main__':
    unittest.main()
