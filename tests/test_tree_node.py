__author__ = 'Bohdan Mushkevych'

import mock
import unittest
from unittest.case import skip

from settings import enable_test_mode
enable_test_mode()

from constants import TOKEN_SITE, TOKEN_CLIENT, PROCESS_SITE_YEARLY, PROCESS_SITE_MONTHLY, \
    PROCESS_SITE_DAILY, PROCESS_SITE_HOURLY, PROCESS_CLIENT_MONTHLY
from synergy.db.model.job import Job
from synergy.system import time_helper
from synergy.scheduler.tree import MultiLevelTree
from synergy.scheduler.tree_node import TreeNode, NodesCompositeState
from synergy.system.time_qualifier import *
from tests.state_machine_testing_utils import TEST_PRESET_TIMEPERIOD


class TestTreeNode(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestTreeNode, cls).setUpClass()

    def setUp(self):
        self.tree_mock = mock.create_autospec(MultiLevelTree)
        self.parent_node_mock = mock.create_autospec(TreeNode)
        self.job_mock = mock.create_autospec(Job)
        self.the_node = TreeNode(self.tree_mock, self.parent_node_mock, PROCESS_SITE_HOURLY,
                                 TEST_PRESET_TIMEPERIOD, self.job_mock)

    def tearDown(self):
        del self.the_node
        del self.job_mock
        del self.tree_mock

    def test_find_counterpart_for(self):
        tree_four_level = MultiLevelTree(process_names=[PROCESS_SITE_YEARLY,
                                                        PROCESS_SITE_MONTHLY,
                                                        PROCESS_SITE_DAILY,
                                                        PROCESS_SITE_HOURLY],
                                         tree_name=TOKEN_SITE)

        tree_two_level = MultiLevelTree(process_names=[PROCESS_CLIENT_MONTHLY],
                                        tree_name=TOKEN_CLIENT)

        tree_four_level.build_tree()
        tree_two_level.build_tree()

        actual_yearly_timeperiod = time_helper.actual_timeperiod(QUALIFIER_YEARLY)
        actual_monthly_timeperiod = time_helper.actual_timeperiod(QUALIFIER_MONTHLY)
        actual_daily_timeperiod = time_helper.actual_timeperiod(QUALIFIER_DAILY)
        actual_hourly_timeperiod = time_helper.actual_timeperiod(QUALIFIER_HOURLY)

        # case 1 - left-to-right successful comparison
        node_a = tree_two_level.get_node(PROCESS_CLIENT_MONTHLY, actual_monthly_timeperiod)
        node_b = node_a.find_counterpart_in(tree_four_level)
        self.assertIsNotNone(node_b)
        self.assertEqual(node_a.timeperiod, node_b.timeperiod)

        # case 2 - right-to-left successful comparison
        node_a = tree_four_level.get_node(PROCESS_SITE_MONTHLY, actual_monthly_timeperiod)
        node_b = node_a.find_counterpart_in(tree_two_level)
        self.assertIsNotNone(node_b)
        self.assertEqual(node_a.timeperiod, node_b.timeperiod)

        # case 3 - right-to-left unsuccessful comparison
        node_a = tree_four_level.get_node(PROCESS_SITE_DAILY, actual_daily_timeperiod)
        node_b = node_a.find_counterpart_in(tree_two_level)
        self.assertIsNone(node_b)

        # case 4 - right-to-left unsuccessful comparison
        node_a = tree_four_level.get_node(PROCESS_SITE_HOURLY, actual_hourly_timeperiod)
        node_b = node_a.find_counterpart_in(tree_two_level)
        self.assertIsNone(node_b)

        # case 5 - right-to-left unsuccessful comparison
        node_a = tree_four_level.get_node(PROCESS_SITE_YEARLY, actual_yearly_timeperiod)
        node_b = node_a.find_counterpart_in(tree_two_level)
        self.assertIsNone(node_b)

    @skip('incomplete test case')
    def test_validate(self):
        # step 0: request Job record if current one is not set
        self.the_node.job_record = None
        self.the_node.request_embryo_job_record = mock.Mock()
        self.the_node.validate()
        self.the_node.request_embryo_job_record.assert_called_once_with()
        self.the_node.job_record = None

    def test_is_finalizable(self):
        self.job_mock.is_active = True

        composite_state = NodesCompositeState()
        composite_state.all_finished = True
        self.the_node.dependent_on_composite_state = mock.Mock(return_value=composite_state)
        self.the_node.request_embryo_job_record = mock.Mock()
        for _index in range(10):
            mock_job = mock.create_autospec(Job)
            mock_job.is_finished = True
            child_mock = mock.create_autospec(TreeNode)
            child_mock.job_record = mock.create_autospec(Job)
            child_mock.job_record.is_active = True
            self.the_node.children[_index] = child_mock

        # happy-flow
        self.assertTrue(self.the_node.is_finalizable())

        # the job record of the node is still active
        self.the_node.job_record.is_active = False
        self.assertFalse(self.the_node.is_finalizable())

        # at least one of dependent nodes is not finished
        self.the_node.job_record.is_active = True
        composite_state.all_finished = False
        self.assertFalse(self.the_node.is_finalizable())

        # at least one child is still active
        composite_state.all_finished = True
        self.the_node.children[0].job_record.is_finished = False
        self.assertFalse(self.the_node.is_finalizable())


if __name__ == '__main__':
    unittest.main()
