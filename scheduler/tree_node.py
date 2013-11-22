
__author__ = 'Bohdan Mushkevych'


from system.process_context import ProcessContext
from model import time_table


class AbstractNode(object):
    def __init__(self, tree, parent, process_name, timestamp, time_record):
        # initializes the data members
        self.children = dict()
        self.tree = tree
        self.parent = parent
        self.process_name = process_name
        self.timestamp = timestamp
        self.time_record = time_record

    def request_reprocess(self):
        """ method marks this and all parents node as such that requires reprocessing
        @return list of nodes that have been effected """
        effected_nodes = []
        if self.parent is None:
            # do not process 'root' - the only node that has None as 'parent'
            return effected_nodes

        for function in self.tree.reprocess_callbacks:
            # function signature: process_name, timestamp, tree_node
            function(self.process_name, self.timestamp, self)
        effected_nodes.extend(self.parent.request_reprocess())
        effected_nodes.append(self)
        return effected_nodes

    def request_skip(self):
        """ method marks this node as one to skip"""
        for function in self.tree.skip_callbacks:
            # function signature: process_name, timestamp, tree_node
            function(self.process_name, self.timestamp, self)
        return [self]

    def request_timetable_record(self):
        """ method is requesting outside functionality to create STATE_EMBRYO timetable record for given tree_node"""
        for function in self.tree.create_timetable_record_callbacks:
            # function signature: process_name, timestamp, tree_node
            function(self.process_name, self.timestamp, self)

    def can_finalize_timetable_record(self):
        """method checks all children of the node, and if any is _not_ finalized - refuses to finalize the node"""
        pass

    def validate(self):
        """method traverse tree and:
        * requests for STATE_EMBRYO timetable records """
        if self.time_record is None:
            self.request_timetable_record()

    def add_log_entry(self, entry):
        """ time_record holds MAX_NUMBER_OF_LOG_ENTRIES of log entries, that can be accessed by MX
            this method adds record and removes oldest one if necessary """
        log = self.time_record.log
        if len(log) > time_table.MAX_NUMBER_OF_LOG_ENTRIES:
            del log[-1]
        log.insert(0, entry)

    def is_dependent_on_finalized(self):
        """ method is used by:
            - horizontal nodes to verify that particular time period has been finalized in verticals
            prior to finalization of the horizontal time-period
            - alerts to make sure they are run only when all data is present and finalized
            - financial daily nodes to make sure that Google Domain and Google Channel data is available
            - financial post processing timperiods to track completion of both financial and traffic data
            @return tuple (dependents_are_finalized, dependents_are_skipped) indicating
                dependents_are_finalized - indicates if all <dependent on> periods are in STATE_PROCESSED
                dependents_are_skipped - indicates that among <dependent on> periods are some in STATE_SKIPPED
             """
        def match_time_qualifier(actual_process_name, candidate_process_name):
            if candidate_process_name is None:
                return False
            time_qualifier = ProcessContext.get_time_qualifier(actual_process_name)
            candidate_qualifier = ProcessContext.get_time_qualifier(candidate_process_name)
            return time_qualifier == candidate_qualifier

        dependents_are_finalized = True  # indicates if all dependent on periods are in STATE_PROCESSED
        dependents_are_skipped = False   # indicates that among dependent on periods are some in STATE_SKIPPED
        for dependent_on in self.tree.dependent_on:
            dep_process_yearly = getattr(dependent_on, 'process_yearly', None)
            dep_process_monthly = getattr(dependent_on, 'process_monthly', None)
            dep_process_daily = getattr(dependent_on, 'process_daily', None)
            dep_process_hourly = getattr(dependent_on, 'process_hourly', None)
            dep_process_linear = getattr(dependent_on, 'process_name', None)

            if match_time_qualifier(self.process_name, dep_process_yearly):
                dep_proc_name = dependent_on.process_yearly
            elif match_time_qualifier(self.process_name, dep_process_monthly):
                dep_proc_name = dependent_on.process_monthly
            elif match_time_qualifier(self.process_name, dep_process_daily):
                dep_proc_name = dependent_on.process_daily
            elif match_time_qualifier(self.process_name, dep_process_hourly):
                dep_proc_name = dependent_on.process_hourly
            elif match_time_qualifier(self.process_name, dep_process_linear):
                dep_proc_name = dependent_on.process_name
            else:
                # special case when tree with more levels depends on the tree with smaller amount of levels
                # for example ThreeLevel Financial tree depends on TwoLevel Google Channel
                # in this case - we just verify time-periods that matches in both trees;
                # for levels that have no match, we assume that dependency does not exists
                # for example Financial Monthly has no counterpart in Google Report - so we assume that its not blocked
                continue

            dep_node = dependent_on.get_node_by_process(dep_proc_name, self.timestamp)
            if dep_node.time_record.state != time_table.STATE_PROCESSED:
                dependents_are_finalized = False
            if dep_node.time_record.state == time_table.STATE_SKIPPED:
                dependents_are_skipped = True

        return dependents_are_finalized, dependents_are_skipped


class LinearNode(AbstractNode):
    def __init__(self, tree, parent, process_name, timestamp, time_record):
        super(LinearNode, self).__init__(tree, parent, process_name, timestamp, time_record)

    def can_finalize_timetable_record(self):
        """ method checks the if this particular node can be finalized: i.e. all dependents are finalized
            and node itself is satisfying success criteria """
        if self.parent is None:
            # this is root node
            return False

        if not self.is_dependent_on_finalized():
            return False

        if self.time_record is None:
            self.request_timetable_record()

        if self.time_record.state == time_table.STATE_FINAL_RUN \
                or self.time_record.state == time_table.STATE_IN_PROGRESS \
                or self.time_record.state == time_table.STATE_EMBRYO:
            return True
        return False


class TreeNode(AbstractNode):
    HOURS_IN_DAY = 24

    def __init__(self, tree, parent, process_name, timestamp, time_record):
        super(TreeNode, self).__init__(tree, parent, process_name, timestamp, time_record)

    def can_finalize_timetable_record(self):
        """method checks:
         - all counterpart of this node in dependent_on trees, if they all are finalized
         - all children of the node, and if any is _not_ finalized - refuses to finalize the node"""

        if not self.is_dependent_on_finalized():
            return False

        if self.time_record is None:
            self.request_timetable_record()

        children_processed = True
        for timestamp in self.children:
            child = self.children[timestamp]
            if child.time_record.state == time_table.STATE_FINAL_RUN \
                    or child.time_record.state == time_table.STATE_IN_PROGRESS \
                    or child.time_record.state == time_table.STATE_EMBRYO:
                children_processed = False
                break
        return children_processed

    def validate(self):
        """method traverse tree and kicks two types of events:
        * requests for STATE_EMBRYO timetable records
        * requests nodes for reprocessing, if STATE_PROCESSED node relies on unfinalized nodes
        * requests node for skipping if it is daily node and all 24 of its Hourly nodes are in STATE_SKIPPED state"""
        super(TreeNode, self).validate()

        all_children_skipped = True
        children_processed = True
        for timestamp in self.children:
            child = self.children[timestamp]
            children_processed = child.validate()

            if child.time_record.state == time_table.STATE_EMBRYO \
                    or child.time_record.state == time_table.STATE_IN_PROGRESS \
                    or child.time_record.state == time_table.STATE_FINAL_RUN:
                children_processed = False
            if child.time_record.state != time_table.STATE_SKIPPED:
                all_children_skipped = False

        if children_processed == False \
            and (self.time_record.state == time_table.STATE_FINAL_RUN
                 or self.time_record.state == time_table.STATE_PROCESSED):
            self.request_reprocess()

        # ideally, we should check if our tree holds HOURLY records by running <isinstance(self.tree, FourLevelTree)>
        # however, we can not import FourLevelTree because of self-reference import issue
        # to solve this problem we check presence of <process_hourly> attribute
        if all_children_skipped \
                and hasattr(self.tree, 'process_hourly') \
                and self.process_name == self.tree.process_daily \
                and len(self.children) == TreeNode.HOURS_IN_DAY:
            self.request_skip()
