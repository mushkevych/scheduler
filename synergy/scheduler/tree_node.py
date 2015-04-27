__author__ = 'Bohdan Mushkevych'

from synergy.db.model import job
from synergy.system import time_helper
from synergy.conf import context


class NodesCompositeState(object):
    """ Instance of this structure represents composite state of TreeNodes """

    def __init__(self):
        super(NodesCompositeState, self).__init__()

        # True if all dependent_on Jobs are finished
        self.all_finished = True

        # True if all dependent_on Jobs are successfully processed
        self.all_processed = True

        # True if all dependent_on Jobs are either active or successfully processed
        self.all_healthy = True

        # True if among dependent_on periods are some in STATE_SKIPPED
        self.skipped_present = False

    def enlist(self, tree_node):
        assert isinstance(tree_node, TreeNode)
        if not tree_node.job_record.is_finished:
            self.all_finished = False
        if not tree_node.job_record.is_processed:
            self.all_processed = False
        if not tree_node.job_record.is_active or not tree_node.job_record.is_processed:
            self.all_healthy = False
        if tree_node.job_record.is_skipped:
            self.skipped_present = True


class TreeNode(object):
    def __init__(self, tree, parent, process_name, timeperiod, job_record):
        # initializes the data members
        self.children = dict()
        self.tree = tree
        self.parent = parent
        self.process_name = process_name
        self.timeperiod = timeperiod
        self.job_record = job_record
        if parent is None and process_name is None and timeperiod is None and job_record is None:
            # special case - node is TREE ROOT
            self.time_qualifier = None
        else:
            self.time_qualifier = context.process_context[self.process_name].time_qualifier

    def request_reprocess(self):
        """ method marks this and all parents node as such that requires reprocessing
        :return list of nodes that have been effected """
        effected_nodes = []
        if self.parent is None:
            # do not process 'root' - the only node that has None as 'parent'
            return effected_nodes

        for function in self.tree.reprocess_callbacks:
            # function signature: tree_node
            function(self)
        effected_nodes.extend(self.parent.request_reprocess())
        effected_nodes.append(self)
        return effected_nodes

    def request_skip(self):
        """ method marks this node as one to skip"""
        for function in self.tree.skip_callbacks:
            # function signature: tree_node
            function(self)
        return [self]

    def request_embryo_job_record(self):
        """ method is requesting outside functionality to create a job record in STATE_EMBRYO for given tree_node """
        for function in self.tree.create_job_record_callbacks:
            # function signature: tree_node
            function(self)

    def is_finalizable(self):
        """method checks whether:
         - all counterpart of this node in dependent_on trees are finished
         - all direct children of the node are finished
         - the node itself is in active state"""

        composite_state = self.dependent_on_composite_state()
        if not composite_state.all_finished:
            return False

        if self.job_record is None:
            self.request_embryo_job_record()

        children_processed = all([child.job_record.is_finished for child in self.children.values()])

        return children_processed and self.job_record.is_active

    def validate(self):
        """method traverse tree and performs following activities:
        * requests a job record in STATE_EMBRYO if no job record is currently assigned to the node
        * requests nodes for reprocessing, if STATE_PROCESSED node relies on unfinalized nodes
        * requests node for skipping if it is daily node and all 24 of its Hourly nodes are in STATE_SKIPPED state"""

        # step 0: request Job record if current one is not set
        if self.job_record is None:
            self.request_embryo_job_record()

        # step 1: define if current node has a younger sibling
        next_timeperiod = time_helper.increment_timeperiod(self.time_qualifier, self.timeperiod)
        has_younger_sibling = next_timeperiod in self.parent.children

        # step 2: define if all children are done and if perhaps they all are in STATE_SKIPPED
        all_children_skipped = True
        all_children_finished = True
        for timeperiod in self.children:
            child = self.children[timeperiod]
            child.validate()

            if child.job_record.is_active:
                all_children_finished = False
            if not child.job_record.is_skipped:
                all_children_skipped = False

        # step 3: request this node's reprocessing if it is enroute to STATE_PROCESSED
        # while some of its children are still performing processing
        if all_children_finished is False and self.job_record.is_finished:
            self.request_reprocess()

        # step 4: verify if this node should be transferred to STATE_SKIPPED
        # algorithm is following:
        # point a: node must have children
        # point b: existence of a younger sibling means that the tree contains another node of the same level
        # thus - should the tree.build_timeperiod be not None - the children level of this node is fully constructed
        # point c: if all children of this node are in STATE_SKIPPED then we will set this node state to STATE_SKIPPED
        if len(self.children) != 0 \
                and all_children_skipped \
                and self.tree.build_timeperiod is not None \
                and has_younger_sibling is True \
                and not self.job_record.is_skipped:
            self.request_skip()

    def add_log_entry(self, entry):
        """ :db.model.job record holds MAX_NUMBER_OF_LOG_ENTRIES of log entries, that can be accessed by MX
            this method adds a record and removes oldest one if necessary """
        log = self.job_record.log
        if len(log) > job.MAX_NUMBER_OF_LOG_ENTRIES:
            del log[-1]
        log.insert(0, entry)

    def find_counterpart_in(self, tree_b):
        """ Finds a TreeNode counterpart for this node in tree_b
        :param tree_b: target tree that hosts counterpart to this node
        :return: TreeNode from tree_b that has the same timeperiod as this node, or None if no counterpart ware found
        """

        tree_b_hierarchy_entry = tree_b.process_hierarchy.get_by_qualifier(self.time_qualifier)
        if not tree_b_hierarchy_entry:
            # special case when tree with more levels depends on the tree with smaller amount of levels
            # for example ThreeLevel Financial tree depends on TwoLevel Google Channel
            # in this case - we just verify time-periods that matches in both trees;
            # for levels that have no match, we assume that dependency does not exists
            # for example Financial Monthly has no counterpart in Google Daily Report -
            # so we assume that its not blocked
            node_b = None
        else:
            node_b = tree_b.get_node(tree_b_hierarchy_entry.process_entry.process_name, self.timeperiod)

        return node_b

    def dependent_on_composite_state(self):
        """ method iterates over all nodes that provide dependency to the current node,
            and compile composite state of them all
            :return instance of <NodesCompositeState>
        """
        composite_state = NodesCompositeState()

        for dependent_on in self.tree.dependent_on:
            node_b = self.find_counterpart_in(dependent_on)

            if node_b is None:
                # special case when counterpart tree has no process with corresponding time_qualifier
                # for example Financial Monthly has no counterpart in Third-party Daily Report -
                # so we assume that its not blocked
                continue

            composite_state.enlist(node_b)

        return composite_state
