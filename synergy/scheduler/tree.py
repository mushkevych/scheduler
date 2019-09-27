__author__ = 'Bohdan Mushkevych'

from datetime import datetime

from synergy.scheduler.process_hierarchy import ProcessHierarchy
from synergy.scheduler.tree_node import TreeNode, RootNode, AbstractTreeNode
from synergy.conf import settings
from synergy.system import time_helper
from synergy.system.time_helper import cast_to_time_qualifier


# number of times a Job can fail before it is considered STATE_SKIPPED.
MAX_NUMBER_OF_FAILURES = 3


class AbstractTree(object):
    """ MixIn to handle subscription for various tree events """

    def __init__(self):
        self.dependent_on = []

    def register_dependent_on(self, tree):
        """registering tree that we are dependent on.
        example: horizontal client should not be finalized until we have finalized vertical site for the same period"""
        self.dependent_on.append(tree)

    def unregister_dependent_on(self, tree):
        """unregistering tree that we are dependent on"""
        if tree in self.dependent_on:
            self.dependent_on.remove(tree)


class MultiLevelTree(AbstractTree):
    """ Multi-level Tree, suited to host both single Process Entry
        or multiple hierarchy-organized Process Entries """

    def __init__(self, process_names, timetable, tree_name=None, mx_name=None, mx_page=None):
        """
        :param tree_name <optional>: full name of the tree. used as an identifier
        :param mx_name <optional>: is used by MX only as visual vertical name
        :param mx_page <optional>: is used by MX only as anchor to specific page
        """
        super(MultiLevelTree, self).__init__()
        self.process_hierarchy = ProcessHierarchy(*process_names)

        self.build_timeperiod = None
        self.validation_timestamp = None
        self.timetable = timetable
        self.tree_name = tree_name
        self.mx_name = mx_name
        self.mx_page = mx_page
        self.root = RootNode(self)

    def __contains__(self, value):
        """
        :param value: process name
        :return: True if a process_entry with the given name is registered in this hierarchy; False otherwise
        """
        return value in self.process_hierarchy

    def _get_next_parent_node(self, parent):
        """ Used by _get_next_child_node, this method is called to find next possible parent.
            For example if timeperiod 2011010200 has all children processed, but is not yet processed itself
            then it makes sense to look in 2011010300 for hourly nodes """
        grandparent = parent.parent
        if grandparent is None:
            # here, we work at yearly/linear level
            return None

        parent_siblings = list(grandparent.children)
        sorted_keys = sorted(parent_siblings)
        index = sorted_keys.index(parent.timeperiod)
        if index + 1 >= len(sorted_keys):
            return None
        else:
            return grandparent.children[sorted_keys[index + 1]]

    def _get_next_child_node(self, parent):
        """
            Iterates among children of the given parent and looks for a suitable node to process
            In case given parent has no suitable nodes, a younger parent will be found
            and the logic will be repeated for him
        """
        children_keys = list(parent.children)
        sorted_keys = sorted(children_keys)
        for key in sorted_keys:
            node = parent.children[key]
            if node.job_record is None:
                self.timetable.assign_job_record(node)
                return node
            elif self.should_skip_tree_node(node):
                continue
            elif node.job_record.is_active:
                return node

        # special case, when all children of the parent node are not suitable for processing
        new_parent = self._get_next_parent_node(parent)
        if new_parent is not None:
            # in case all nodes are processed or blocked - look for next valid parent node
            return self._get_next_child_node(new_parent)
        else:
            # if all valid parents are exploited - return current node
            process_name = parent.children[sorted_keys[0]].process_name
            time_qualifier = parent.children[sorted_keys[0]].time_qualifier
            actual_timeperiod = time_helper.actual_timeperiod(time_qualifier)
            return self.get_node(process_name, actual_timeperiod)

    def _get_node(self, time_qualifier, timeperiod):
        """
        Method retrieves a tree node identified by time_qualifier and timeperiod
        In case intermittent or target nodes does not exist - method will request their creation
        :param time_qualifier: identifies the tree level
        :param timeperiod: identifies the target node timeperiod
        :return: requested node; type <AbstractNode>
        """
        hierarchy_entry = self.process_hierarchy.get_by_qualifier(time_qualifier)
        if hierarchy_entry.parent:
            parent_time_qualifier = hierarchy_entry.parent.process_entry.time_qualifier
            parent_timeperiod = hierarchy_entry.parent.cast_timeperiod(timeperiod)
            parent = self._get_node(parent_time_qualifier, parent_timeperiod)
        else:
            parent = self.root

        node = parent.children.get(timeperiod)
        if node is None:
            node = TreeNode(self, parent, hierarchy_entry.process_entry.process_name, timeperiod, None)
            parent.children[timeperiod] = node

        return node

    def _get_next_node(self, time_qualifier):
        """ Method goes to the top of the tree and traverses from
            there in search of the next suitable node for processing
            to the level defined by the given time_qualifier
            :param time_qualifier: defines target level of the tree
            :return: located node; type <TreeNode> """
        hierarchy_entry = self.process_hierarchy.get_by_qualifier(time_qualifier)
        if hierarchy_entry.parent:
            parent_time_qualifier = hierarchy_entry.parent.process_entry.time_qualifier
            parent = self._get_next_node(parent_time_qualifier)
        else:
            parent = self.root

        return self._get_next_child_node(parent)

    def should_skip_tree_node(self, node: AbstractTreeNode):
        """ :return True: in case the node should be _skipped_ and not included into processing """
        # case 1: node processing is complete
        if node.job_record.is_finished:
            return True

        # case 2: this is a bottom-level leaf node. retry this node for MAX_NUMBER_OF_FAILURES
        if node.process_name == self.process_hierarchy.bottom_process.process_name:
            if len(node.children) == 0:
                # no children - this is a leaf
                return node.job_record.number_of_failures > MAX_NUMBER_OF_FAILURES

        # case 3: here we process process_daily, process_monthly and process_yearly that have children
        # iterate thru children and check if all of them are in STATE_SKIPPED (i.e. no data for parent to process)
        # if any is still in processing (i.e. has produced some data) - then we can not skip parent of the child node
        # case 3': consider parent as worth processing (i.e. do not skip) if child's job_record is None
        all_children_spoiled = True
        for key, child in node.children.items():
            if child.job_record is None or \
                    (child.job_record.number_of_failures <= MAX_NUMBER_OF_FAILURES
                     and not child.job_record.is_skipped):
                all_children_spoiled = False
                break
        return all_children_spoiled

    def build_tree(self, rebuild=False):
        """ method builds tree by iterating from the synergy_start_timeperiod to the current time
            and inserting corresponding nodes """

        time_qualifier = self.process_hierarchy.bottom_process.time_qualifier
        process_name = self.process_hierarchy.bottom_process.process_name
        if rebuild or self.build_timeperiod is None:
            timeperiod = settings.settings['synergy_start_timeperiod']
        else:
            timeperiod = self.build_timeperiod

        timeperiod = cast_to_time_qualifier(time_qualifier, timeperiod)
        actual_timeperiod = time_helper.actual_timeperiod(time_qualifier)

        while actual_timeperiod >= timeperiod:
            self.get_node(process_name, timeperiod)
            timeperiod = time_helper.increment_timeperiod(time_qualifier, timeperiod)

        self.build_timeperiod = actual_timeperiod

    def get_next_node(self, process_name):
        """ :return: <AbstractTreeNode> next node to process by a process with process_name """
        if process_name not in self.process_hierarchy:
            raise ValueError(f'unable to compute the next_node due to unknown process: {process_name}')

        time_qualifier = self.process_hierarchy[process_name].process_entry.time_qualifier
        return self._get_next_node(time_qualifier)

    def update_node(self, job_record):
        """ Updates job record property for a tree node associated with the given Job """
        if job_record.process_name not in self.process_hierarchy:
            raise ValueError(f'unable to update the node due to unknown process: {job_record.process_name}')

        time_qualifier = self.process_hierarchy[job_record.process_name].process_entry.time_qualifier
        node = self._get_node(time_qualifier, job_record.timeperiod)
        node.job_record = job_record

    def get_node(self, process_name, timeperiod):
        """ Method retrieves a tree node identified by the time_qualifier and the timeperiod """
        if process_name not in self.process_hierarchy:
            raise ValueError(f'unable to retrieve the node due to unknown process: {process_name}')

        time_qualifier = self.process_hierarchy[process_name].process_entry.time_qualifier
        return self._get_node(time_qualifier, timeperiod)

    def validate(self):
        """ method starts validation of the tree.
            @see TreeNode.validate """
        for timeperiod, child in self.root.children.items():
            child.validate()
        self.validation_timestamp = datetime.utcnow()
