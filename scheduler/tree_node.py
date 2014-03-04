__author__ = 'Bohdan Mushkevych'

from db.model import time_table_record
from system import time_helper
from system.process_context import ProcessContext


class AbstractNode(object):
    def __init__(self, tree, parent, process_name, timeperiod, timetable_record):
        # initializes the data members
        self.children = dict()
        self.tree = tree
        self.parent = parent
        self.process_name = process_name
        self.timeperiod = timeperiod
        self.timetable_record = timetable_record
        if parent is None and process_name is None and timeperiod is None and timetable_record is None:
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
            # function signature: process_name, timeperiod, tree_node
            function(self.process_name, self.timeperiod, self)
        effected_nodes.extend(self.parent.request_reprocess())
        effected_nodes.append(self)
        return effected_nodes

    def request_skip(self):
        """ method marks this node as one to skip"""
        for function in self.tree.skip_callbacks:
            # function signature: process_name, timeperiod, tree_node
            function(self.process_name, self.timeperiod, self)
        return [self]

    def request_timetable_record(self):
        """ method is requesting outside functionality to create STATE_EMBRYO timetable record for given tree_node"""
        for function in self.tree.create_timetable_record_callbacks:
            # function signature: process_name, timeperiod, tree_node
            function(self.process_name, self.timeperiod, self)

    def can_finalize_timetable_record(self):
        """method checks all children of the node, and if any is _not_ finalized - refuses to finalize the node"""
        pass

    def validate(self):
        """method traverse tree and:
        * requests for timetable records in STATE_EMBRYO if no timetable record is currently assigned"""
        if self.timetable_record is None:
            self.request_timetable_record()

    def add_log_entry(self, entry):
        """ timetable_record holds MAX_NUMBER_OF_LOG_ENTRIES of log entries, that can be accessed by MX
            this method adds record and removes oldest one if necessary """
        log = self.timetable_record.log
        if len(log) > time_table_record.MAX_NUMBER_OF_LOG_ENTRIES:
            del log[-1]
        log.insert(0, entry)

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

        def match_time_qualifier(time_qualifier, candidate_process_name):
            """ :return: True if candidate_process has the same time qualifier as given """
            if candidate_process_name is None:
                return False
            candidate_qualifier = ProcessContext.get_time_qualifier(candidate_process_name)
            return time_qualifier == candidate_qualifier

        all_finalized = True   # True if all dependent_on periods are either in STATE_PROCESSED or STATE_SKIPPED
        all_processed = True   # True if all dependent_on periods are in STATE_PROCESSED
        skipped_present = False   # True if among dependent_on periods are some in STATE_SKIPPED
        for dependent_on in self.tree.dependent_on:
            dep_process_yearly = getattr(dependent_on, 'process_yearly', None)
            dep_process_monthly = getattr(dependent_on, 'process_monthly', None)
            dep_process_daily = getattr(dependent_on, 'process_daily', None)
            dep_process_hourly = getattr(dependent_on, 'process_hourly', None)
            dep_process_linear = getattr(dependent_on, 'process_name', None)

            if match_time_qualifier(self.time_qualifier, dep_process_yearly):
                dep_proc_name = dependent_on.process_yearly
            elif match_time_qualifier(self.time_qualifier, dep_process_monthly):
                dep_proc_name = dependent_on.process_monthly
            elif match_time_qualifier(self.time_qualifier, dep_process_daily):
                dep_proc_name = dependent_on.process_daily
            elif match_time_qualifier(self.time_qualifier, dep_process_hourly):
                dep_proc_name = dependent_on.process_hourly
            elif match_time_qualifier(self.time_qualifier, dep_process_linear):
                dep_proc_name = dependent_on.process_name
            else:
                # special case when tree with more levels depends on the tree with smaller amount of levels
                # for example ThreeLevel Financial tree depends on TwoLevel Google Channel
                # in this case - we just verify time-periods that matches in both trees;
                # for levels that have no match, we assume that dependency does not exists
                # for example Financial Monthly has no counterpart in Google Report - so we assume that its not blocked
                continue

            dep_node = dependent_on.get_node_by_process(dep_proc_name, self.timeperiod)
            if dep_node.timetable_record.state not in [time_table_record.STATE_PROCESSED,
                                                       time_table_record.STATE_SKIPPED]:
                all_finalized = False
            if dep_node.timetable_record.state != time_table_record.STATE_PROCESSED:
                all_processed = False
            if dep_node.timetable_record.state == time_table_record.STATE_SKIPPED:
                skipped_present = True

        return all_finalized, all_processed, skipped_present


class LinearNode(AbstractNode):
    def __init__(self, tree, parent, process_name, timeperiod, timetable_record):
        super(LinearNode, self).__init__(tree, parent, process_name, timeperiod, timetable_record)

    def can_finalize_timetable_record(self):
        """ method checks the if this particular node can be finalized: i.e. all dependents are finalized
            and node itself is satisfying success criteria """
        if self.parent is None:
            # this is root node
            return False

        all_finalized, all_processed, skipped_present = self.is_dependent_on_finalized()
        if not all_finalized:
            return False

        if self.timetable_record is None:
            self.request_timetable_record()

        if self.timetable_record.state in [time_table_record.STATE_FINAL_RUN,
                                           time_table_record.STATE_IN_PROGRESS,
                                           time_table_record.STATE_EMBRYO]:
            return True
        return False


class TreeNode(AbstractNode):
    """ TreeNode is used to operate a node in a tree where all levels are equal and are represented by fully-operational
        processes.
        For instance: ThreeLevelTree, FourLevelTree"""

    def __init__(self, tree, parent, process_name, timeperiod, timetable_record):
        super(TreeNode, self).__init__(tree, parent, process_name, timeperiod, timetable_record)

    def can_finalize_timetable_record(self):
        """method checks:
         - all counterpart of this node in dependent_on trees, if they all are finalized
         - all children of the node, and if any is _not_ finalized - refuses to finalize the node"""

        all_finalized, all_processed, skipped_present = self.is_dependent_on_finalized()
        if not all_finalized:
            return False

        if self.timetable_record is None:
            self.request_timetable_record()

        children_processed = True
        for timeperiod in self.children:
            child = self.children[timeperiod]
            if child.timetable_record.state in [time_table_record.STATE_FINAL_RUN,
                                                time_table_record.STATE_IN_PROGRESS,
                                                time_table_record.STATE_EMBRYO]:
                children_processed = False
                break
        return children_processed

    def validate(self):
        """method traverse tree and performs following activities:
        * requests for timetable records in STATE_EMBRYO if no timetable record is currently assigned
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

            if child.timetable_record.state in [time_table_record.STATE_EMBRYO,
                                                time_table_record.STATE_IN_PROGRESS,
                                                time_table_record.STATE_FINAL_RUN]:
                all_children_done = False
            if child.timetable_record.state != time_table_record.STATE_SKIPPED:
                all_children_skipped = False

        # step 3: request this node's reprocessing if it is enroute to STATE_PROCESSED
        # while some of its children are still performing processing
        if all_children_done is False \
            and self.timetable_record.state in [time_table_record.STATE_FINAL_RUN, time_table_record.STATE_PROCESSED]:
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
            and self.timetable_record.state != time_table_record.STATE_SKIPPED:
            self.request_skip()
