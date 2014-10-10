__author__ = 'Bohdan Mushkevych'

from synergy.db.model import job
from synergy.system import time_helper
from synergy.conf.process_context import ProcessContext


class AbstractNode(object):
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
            self.time_qualifier = ProcessContext.get_time_qualifier(self.process_name)

    def request_reprocess(self):
        """ method marks this and all parents node as such that requires reprocessing
        @return list of nodes that have been effected """
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

    def can_finalize_job_record(self):
        """ method checks all children of the node, and if any is _not_ finalized - refuses to finalize the node """
        pass

    def validate(self):
        """method traverse tree and:
        * requests a job record in STATE_EMBRYO if no job record is currently assigned to the node """
        if self.job_record is None:
            self.request_embryo_job_record()

    def add_log_entry(self, entry):
        """ :db.model.job record holds MAX_NUMBER_OF_LOG_ENTRIES of log entries, that can be accessed by MX
            this method adds a record and removes oldest one if necessary """
        log = self.job_record.log
        if len(log) > job.MAX_NUMBER_OF_LOG_ENTRIES:
            del log[-1]
        log.insert(0, entry)

    @staticmethod
    def find_counterpart_for(node_a, tree_b):
        """ Finds a counterpart TreeNode from tree_b to node_a from tree_a
        @param node_a: source node from tree_a
        @param tree_b: target tree that hosts counterpart to node_a
        @return: TreeNode from tree_b that has the same timeperiod as the node_a, or None if no counterpart ware found
        """

        def match_time_qualifier(time_qualifier, candidate_process_name):
            """ :return: True if candidate_process has the same time qualifier as given """
            if candidate_process_name is None:
                return False
            candidate_qualifier = ProcessContext.get_time_qualifier(candidate_process_name)
            return time_qualifier == candidate_qualifier

        tree_b_process_yearly = getattr(tree_b, 'process_yearly', None)
        tree_b_process_monthly = getattr(tree_b, 'process_monthly', None)
        tree_b_process_daily = getattr(tree_b, 'process_daily', None)
        tree_b_process_hourly = getattr(tree_b, 'process_hourly', None)
        tree_b_process_linear = getattr(tree_b, 'process_name', None)

        if match_time_qualifier(node_a.time_qualifier, tree_b_process_yearly):
            tree_b_process_name = tree_b_process_yearly
        elif match_time_qualifier(node_a.time_qualifier, tree_b_process_monthly):
            tree_b_process_name = tree_b_process_monthly
        elif match_time_qualifier(node_a.time_qualifier, tree_b_process_daily):
            tree_b_process_name = tree_b_process_daily
        elif match_time_qualifier(node_a.time_qualifier, tree_b_process_hourly):
            tree_b_process_name = tree_b_process_hourly
        elif match_time_qualifier(node_a.time_qualifier, tree_b_process_linear):
            tree_b_process_name = tree_b_process_linear
        else:
            # special case when tree with more levels depends on the tree with smaller amount of levels
            # for example ThreeLevel Financial tree depends on TwoLevel Google Channel
            # in this case - we just verify time-periods that matches in both trees;
            # for levels that have no match, we assume that dependency does not exists
            # for example Financial Monthly has no counterpart in Google Daily Report -
            # so we assume that its not blocked
            tree_b_process_name = None

        if tree_b_process_name is not None:
            node_b = tree_b.get_node_by_process(tree_b_process_name, node_a.timeperiod)
        else:
            node_b = None
        return node_b

    def is_dependent_on_finalized(self):
        """ method is used by:
            - horizontal nodes to verify that particular time period has been finalized in verticals
            prior to finalization of the horizontal time-period
            - alerts to make sure they are run only when all data is present and finalized
            - financial daily nodes to make sure that input data is available
            - financial post processing timeperiods to track completion of both financial and traffic data
            @return tuple (all_finalized, all_processed, skipped_present) indicating
                all_finalized - True if all <dependent on> periods are in STATE_PROCESSED or STATE_SKIPPED
                all_processed - True if all <dependent on> periods are in STATE_PROCESSED
                skipped_present - True if among <dependent on> periods are some in STATE_SKIPPED
             """

        all_finalized = True   # True if all dependent_on periods are either in STATE_PROCESSED or STATE_SKIPPED
        all_processed = True   # True if all dependent_on periods are in STATE_PROCESSED
        skipped_present = False   # True if among dependent_on periods are some in STATE_SKIPPED
        for dependent_on in self.tree.dependent_on:
            node_b = AbstractNode.find_counterpart_for(self, dependent_on)

            if node_b is None:
                # special case when counterpart tree has no process with corresponding time_qualifier
                # for example Financial Monthly has no counterpart in Google Daily Report -
                # so we assume that its not blocked
                continue

            if node_b.job_record.state not in [job.STATE_PROCESSED, job.STATE_SKIPPED]:
                all_finalized = False
            if node_b.job_record.state != job.STATE_PROCESSED:
                all_processed = False
            if node_b.job_record.state == job.STATE_SKIPPED:
                skipped_present = True

        return all_finalized, all_processed, skipped_present


class LinearNode(AbstractNode):
    def __init__(self, tree, parent, process_name, timeperiod, job_record):
        super(LinearNode, self).__init__(tree, parent, process_name, timeperiod, job_record)

    def can_finalize_job_record(self):
        """ method checks the if this particular node can be finalized: i.e. all dependents are finalized
            and node itself is satisfying success criteria """
        if self.parent is None:
            # this is root node
            return False

        all_finalized, all_processed, skipped_present = self.is_dependent_on_finalized()
        if not all_finalized:
            return False

        if self.job_record is None:
            self.request_embryo_job_record()

        if self.job_record.state in [job.STATE_FINAL_RUN, job.STATE_IN_PROGRESS, job.STATE_EMBRYO]:
            return True
        return False


class TreeNode(AbstractNode):
    """ TreeNode is used to operate a node in a tree where all levels are equal and are represented by fully-operational
        processes.
        For instance: ThreeLevelTree, FourLevelTree"""

    def __init__(self, tree, parent, process_name, timeperiod, job_record):
        super(TreeNode, self).__init__(tree, parent, process_name, timeperiod, job_record)

    def can_finalize_job_record(self):
        """method checks:
         - all counterpart of this node in dependent_on trees, if they all are finalized
         - all children of the node, and if any is _not_ finalized - refuses to finalize the node"""

        all_finalized, all_processed, skipped_present = self.is_dependent_on_finalized()
        if not all_finalized:
            return False

        if self.job_record is None:
            self.request_embryo_job_record()

        children_processed = True
        for timeperiod in self.children:
            child = self.children[timeperiod]
            if child.job_record.state in [job.STATE_FINAL_RUN, job.STATE_IN_PROGRESS, job.STATE_EMBRYO]:
                children_processed = False
                break
        return children_processed

    def validate(self):
        """method traverse tree and performs following activities:
        * requests a job record in STATE_EMBRYO if no job record is currently assigned to the node
        * requests nodes for reprocessing, if STATE_PROCESSED node relies on unfinalized nodes
        * requests node for skipping if it is daily node and all 24 of its Hourly nodes are in STATE_SKIPPED state"""
        super(TreeNode, self).validate()

        # step 1: define if current node has a younger sibling
        next_timeperiod = time_helper.increment_timeperiod(self.time_qualifier, self.timeperiod)
        has_younger_sibling = next_timeperiod in self.parent.children

        # step 2: define if all children are done and if perhaps they all are in STATE_SKIPPED
        all_children_skipped = True
        all_children_done = True
        for timeperiod in self.children:
            child = self.children[timeperiod]
            child.validate()

            if child.job_record.state in [job.STATE_EMBRYO, job.STATE_IN_PROGRESS, job.STATE_FINAL_RUN]:
                all_children_done = False
            if child.job_record.state != job.STATE_SKIPPED:
                all_children_skipped = False

        # step 3: request this node's reprocessing if it is enroute to STATE_PROCESSED
        # while some of its children are still performing processing
        if all_children_done is False \
                and self.job_record.state in [job.STATE_FINAL_RUN, job.STATE_PROCESSED]:
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
                and self.job_record.state != job.STATE_SKIPPED:
            self.request_skip()
