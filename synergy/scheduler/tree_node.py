__author__ = 'Bohdan Mushkevych'

from logging import INFO

from synergy.db.model import job
from synergy.system import time_helper
from synergy.system.immutable_dict import ImmutableDict
from synergy.conf import context


class DependentOnSummary(object):
    """ This structure is compiled to represent a composite state of dependent_on TreeNodes """

    def __init__(self, tree_node):
        self.tree_node = tree_node

        # contains TreeNodes whose Job are not in finished state
        self.unfinished = []

        # contains TreeNodes whose Job are not in [STATE_PROCESSED, STATE_NOOP]
        self.unprocessed = []

        # contains TreeNodes whose Job neither active nor in [STATE_PROCESSED, STATE_NOOP]
        self.unhealthy = []

        # contains TreeNodes whose Job are in STATE_SKIPPED
        self.skipped = []

    def enlist(self, tree_node):
        assert isinstance(tree_node, TreeNode)
        if not tree_node.job_record.is_finished:
            self.unfinished.append(tree_node)
        if not (tree_node.job_record.is_processed or tree_node.job_record.is_noop):
            self.unprocessed.append(tree_node)
        if not (tree_node.job_record.is_active or tree_node.job_record.is_processed or tree_node.job_record.is_noop):
            self.unhealthy.append(tree_node)
        if tree_node.job_record.is_skipped:
            self.skipped.append(tree_node)

    @property
    def all_finished(self):
        # True if all dependent_on Jobs are finished
        return len(self.unfinished) == 0

    @property
    def all_processed(self):
        # True if all dependent_on Jobs are in [STATE_PROCESSED, STATE_NOOP]
        return len(self.unprocessed) == 0

    @property
    def all_healthy(self):
        # True if all dependent_on Jobs are either active or in [STATE_PROCESSED, STATE_NOOP]
        return len(self.unhealthy) == 0

    @property
    def skipped_present(self):
        # True if among dependent_on periods are some in STATE_SKIPPED
        return len(self.skipped) != 0

    def _write_log(self, level:int, msg:str):
        from synergy.system.system_logger import get_logger
        from synergy.scheduler.scheduler_constants import PROCESS_SCHEDULER
        logger = get_logger(PROCESS_SCHEDULER)
        logger.log(level, msg)
        self.tree_node.add_log_entry(msg)

    def _build_str(self, collection:list, description:str):
        blockers = ','.join([str(e) for e in collection])
        return f'TreeNode {self.tree_node} {description}: {blockers}'

    def log_unfinished(self, level:int):
        _summary = self._build_str(self.unfinished, 'is blocked by unfinished:')
        self._write_log(level, _summary)

    def log_unprocessed(self, level:int):
        _summary = self._build_str(self.unprocessed, 'is blocked by unprocessed:')
        self._write_log(level, _summary)

    def log_skipped(self, level:int):
        _summary = self._build_str(self.skipped, 'has skipped among its dependent_on:')
        self._write_log(level, _summary)


class AbstractTreeNode(object):
    def __init__(self, tree, parent, process_name, timeperiod, job_record):
        self.tree = tree
        self.parent = parent
        self.process_name = process_name
        self.timeperiod = timeperiod
        self.job_record = job_record

        # fields self.time_qualifier and self.children are properly set in the child class
        self.time_qualifier = None
        self.children = ImmutableDict({})

    def is_finalizable(self):
        """method checks whether:
         - all counterpart of this node in dependent_on trees are finished
         - all direct children of the node are finished
         - the node itself is in active state"""

        depon_summary = self.dependent_on_summary()
        if not depon_summary.all_finished:
            depon_summary.log_unfinished(INFO)
            return False

        if self.job_record is None:
            self.tree.timetable.assign_job_record(self)

        children_processed = all([child.job_record.is_finished for child in self.children.values()])

        return children_processed and self.job_record.is_active

    def validate(self):
        """method traverse tree and performs following activities:
        * requests a job record in STATE_EMBRYO if no job record is currently assigned to the node
        * requests nodes for reprocessing, if STATE_PROCESSED node relies on unfinalized nodes
        * requests node for skipping if it is daily node and all 24 of its Hourly nodes are in STATE_SKIPPED state"""

        # step 1: request Job record if current one is not set
        if self.job_record is None:
            self.tree.timetable.assign_job_record(self)

        # step 2: define if current node has a younger sibling
        next_timeperiod = time_helper.increment_timeperiod(self.time_qualifier, self.timeperiod)
        has_younger_sibling = next_timeperiod in self.parent.children

        # step 3: define if all children are done and if perhaps they all are in STATE_SKIPPED
        all_children_skipped = True
        all_children_finished = True
        for timeperiod, child in self.children.items():
            child.validate()

            if child.job_record.is_active:
                all_children_finished = False
            if not child.job_record.is_skipped:
                all_children_skipped = False

        # step 4: request this node's reprocessing if it is enroute to STATE_PROCESSED
        # while some of its children are still performing processing
        if all_children_finished is False and self.job_record.is_finished:
            self.tree.timetable.reprocess_tree_node(self)

        # step 5: verify if this node should be transferred to STATE_SKIPPED
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
            self.tree.timetable.skip_tree_node(self)

    def add_log_entry(self, entry):
        """ :db.model.job record holds event log, that can be accessed by MX
            this method adds a record and removes oldest one if necessary """
        event_log = self.job_record.event_log
        if len(event_log) > job.EVENT_LOG_MAX_SIZE:
            del event_log[-1]
        event_log.insert(0, entry)

    def find_counterpart_in(self, tree_b):
        """ Finds a TreeNode counterpart for this node in tree_b
            :param tree_b: target tree that hosts counterpart to this node
            :return: TreeNode from tree_b that has the same timeperiod as self.timeperiod,
                or None if no counterpart ware found
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

    def dependent_on_summary(self):
        """ method iterates over all nodes that provide dependency to the current node,
            and compile composite state of them all
            :return instance of <tree_node.DependencySummary>
        """
        dependency_summary = DependentOnSummary(self)

        for dependent_on in self.tree.dependent_on:
            node_b = self.find_counterpart_in(dependent_on)

            if node_b is None:
                # special case when counterpart tree has no process with corresponding time_qualifier
                # for example Financial Monthly has no counterpart in Third-party Daily Report -
                # so we assume that it's not blocked
                continue

            dependency_summary.enlist(node_b)

        return dependency_summary


class TreeNode(AbstractTreeNode):
    def __init__(self, tree, parent, process_name, timeperiod, job_record):
        super(TreeNode, self).__init__(tree, parent, process_name, timeperiod, job_record)
        self.time_qualifier = context.process_context[process_name].time_qualifier

        child_hierarchy_entry = tree.process_hierarchy.get_child_by_qualifier(self.time_qualifier)
        if child_hierarchy_entry:
            children = dict()
        else:
            # this is the bottom process of the process hierarchy with no children
            children = ImmutableDict({})
        self.children = children

    def __str__(self) -> str:
        state = self.job_record.state if self.job_record else 'unknown'
        return f'{self.tree.tree_name}.{self.process_name}@{self.timeperiod}({state})'


class RootNode(AbstractTreeNode):
    def __init__(self, tree):
        super(RootNode, self).__init__(tree, None, None, None, None)
        self.time_qualifier = None
        self.children = dict()

    def __str__(self) -> str:
        return f'{self.tree.tree_name}.root'
