__author__ = 'Bohdan Mushkevych'

from datetime import datetime
from threading import RLock

from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.dao.job_dao import JobDao
from synergy.db.model.job import Job
from synergy.db.model import job, unit_of_work

from process_starter import get_class
from synergy.conf import context
from synergy.conf import settings
from synergy.system import time_helper
from synergy.system.time_qualifier import *
from synergy.system.decorator import thread_safe
from synergy.scheduler.scheduler_constants import COLLECTION_JOB_HOURLY, COLLECTION_JOB_DAILY, \
    COLLECTION_JOB_MONTHLY, COLLECTION_JOB_YEARLY
from synergy.scheduler.tree import AbstractTree
from synergy.scheduler.tree_node import AbstractNode


class Timetable(object):
    """ Timetable holds all known process trees, where every node presents a timeperiod-driven job"""

    CONTEXT = context.timetable_context

    def __init__(self, logger):
        self.lock = RLock()
        self.logger = logger
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.job_dao = JobDao(self.logger)
        self.reprocess = dict()

        # self.trees contain all of the trees and manages much of their life cycle
        # remember to enlist there all trees the system is working with
        self.trees = self._construct_trees_from_context()

        self._register_callbacks()
        self._register_dependencies()
        self.load_tree()
        self.build_trees()
        self.validate()

    def _construct_trees_from_context(self):
        trees = dict()
        for tree_name, context_entry in self.CONTEXT.iteritems():
            _, tree_klass, _ = get_class(context_entry.tree_classname)
            tree = tree_klass(*context_entry.enclosed_processes,
                              full_name=tree_name,
                              mx_name=context_entry.mx_name,
                              mx_page=context_entry.mx_page)
            trees[tree_name] = tree
        return trees

    def _register_dependencies(self):
        """ register dependencies between trees"""
        for tree_name, context_entry in self.CONTEXT.iteritems():
            tree = self.trees[tree_name]
            assert isinstance(tree, AbstractTree)
            for dependent_on in context_entry.dependent_on:
                dependent_on_tree = self.trees[dependent_on]
                assert isinstance(dependent_on_tree, AbstractTree)
                tree.register_dependent_on(dependent_on_tree)

    def _register_callbacks(self):
        """ register logic that reacts on reprocessing request
        and create embryo timetable record request"""

        # reprocessing request
        for tree_name, tree in self.trees.iteritems():
            tree.register_reprocess_callback(self._callback_reprocess)

        # skip request
        for tree_name, tree in self.trees.iteritems():
            tree.register_skip_callback(self._callback_skip)

        # callbacks register
        for tree_name, tree in self.trees.iteritems():
            tree.register_create_callbacks(self._callback_create_job_record)

    # *** Call-back methods ***
    def _find_dependant_trees(self, tree_obj):
        """ returns list of trees that are dependent_on given tree_obj """
        dependant_trees = []
        for tree_name, tree in self.trees.iteritems():
            if tree_obj in tree.dependent_on:
                dependant_trees.append(tree)
        return dependant_trees

    def _find_dependant_tree_nodes(self, node_a):
        dependant_nodes = set()
        for tree_b in self._find_dependant_trees(node_a.tree):
            node_b = AbstractNode.find_counterpart_for(node_a, tree_b)
            if node_b is None:
                continue
            dependant_nodes.add(node_b)
        return dependant_nodes

    def _reprocess_single_tree_node(self, tree_node):
        """ is called from tree to answer reprocessing request.
        It is possible that timetable record will be transferred to STATE_IN_PROGRESS with no related unit_of_work"""
        uow_id = tree_node.job_record.related_unit_of_work
        if uow_id is not None:
            tree_node.job_record.state = job.STATE_IN_PROGRESS
            uow_obj = self.uow_dao.get_one(uow_id)
            uow_obj.state = unit_of_work.STATE_INVALID
            uow_obj.number_of_retries = 0
            uow_obj.created_at = datetime.utcnow()
            self.uow_dao.update(uow_obj)
            msg = 'Transferred job record %s for %s in timeperiod %s to %s; Transferred unit_of_work to %s' \
                  % (tree_node.job_record.db_id,
                     tree_node.process_name,
                     tree_node.job_record.timeperiod,
                     tree_node.job_record.state,
                     uow_obj.state)

            if tree_node.process_name not in self.reprocess:
                self.reprocess[tree_node.process_name] = dict()
            self.reprocess[tree_node.process_name][tree_node.timeperiod] = tree_node

        else:
            tree_node.job_record.state = job.STATE_EMBRYO
            msg = 'Transferred job record %s for %s in timeperiod %s to %s;' \
                  % (tree_node.job_record.db_id,
                     tree_node.process_name,
                     tree_node.job_record.timeperiod,
                     tree_node.job_record.state)

        tree_node.job_record.number_of_failures = 0
        self.job_dao.update(tree_node.job_record)
        self.logger.warning(msg)
        tree_node.add_log_entry([datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), msg])

    @thread_safe
    def _callback_reprocess(self, tree_node):
        """ is called from tree to answer reprocessing request.
        It is possible that job record will be transferred to STATE_IN_PROGRESS with no related unit_of_work"""
        if (tree_node.job_record.state == job.STATE_EMBRYO
                and tree_node.job_record.number_of_failures == 0) \
            or (tree_node.process_name in self.reprocess
                and tree_node.timeperiod in self.reprocess[tree_node.process_name]):
            # the node has already been marked for re-processing or does not require one
            pass
        else:
            self._reprocess_single_tree_node(tree_node)

        reprocessing_nodes = self._find_dependant_tree_nodes(tree_node)
        for node in reprocessing_nodes:
            node.request_reprocess()

    @thread_safe
    def _callback_skip(self, tree_node):
        """ is called from tree to answer skip request"""
        tree_node.job_record.state = job.STATE_SKIPPED
        uow_id = tree_node.job_record.related_unit_of_work
        if uow_id is not None:
            uow_obj = self.uow_dao.get_one(uow_id)
            uow_obj.state = unit_of_work.STATE_CANCELED
            self.uow_dao.update(uow_obj)
            msg = 'Transferred job record %s in timeperiod %s to %s; Transferred unit_of_work to %s' \
                  % (tree_node.job_record.db_id,
                     tree_node.job_record.timeperiod,
                     tree_node.job_record.state,
                     uow_obj.state)
        else:
            msg = 'Transferred job record %s in timeperiod %s to %s;' \
                  % (tree_node.job_record.db_id,
                     tree_node.job_record.timeperiod,
                     tree_node.job_record.state)

        self.job_dao.update(tree_node.job_record)
        self.logger.warning(msg)
        tree_node.add_log_entry([datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), msg])

        if tree_node.process_name in self.reprocess \
                and tree_node.timeperiod in self.reprocess[tree_node.process_name]:
            del self.reprocess[tree_node.process_name][tree_node.timeperiod]

    @thread_safe
    def _callback_create_job_record(self, tree_node):
        """ is called from a tree to create job record in STATE_EMBRYO and bind it to the given tree node"""

        try:
            job_record = self.job_dao.get_one(tree_node.process_name, tree_node.timeperiod)
        except LookupError:
            job_record = Job()
            job_record.state = job.STATE_EMBRYO
            job_record.timeperiod = tree_node.timeperiod
            job_record.process_name = tree_node.process_name

            tr_id = self.job_dao.update(job_record)
            self.logger.info('Created job record %s, with timeperiod %s for process %s'
                             % (str(tr_id), tree_node.timeperiod, tree_node.process_name))
        tree_node.job_record = job_record

    # *** Tree-manipulation methods ***
    @thread_safe
    def get_tree(self, process_name):
        """ return tree that is managing time-periods for given process"""
        for tree_name, tree in self.trees.iteritems():
            if tree.is_managing_process(process_name):
                return tree

    @thread_safe
    def _build_tree_by_level(self, collection_name, since):
        """ method iterated thru all documents in all job collections and builds a tree of known system state"""
        try:
            unsupported_records = dict()
            document_list = self.job_dao.get_all(collection_name, since)
            for document in document_list:
                tree = self.get_tree(document.process_name)
                if tree is not None:
                    tree.update_node_by_process(document.process_name, document)
                else:
                    unsupported_records[document.process_name] = unsupported_records.get(document.process_name, 0) + 1

            for name, counter in unsupported_records.items():
                self.logger.warning('Skipping %r Job records for %s as no tree is handling it.' % (counter, name))

        except LookupError:
            self.logger.warning('No Job Records in %s.' % str(collection_name))

    @thread_safe
    def load_tree(self):
        """ method iterates thru all objects older than synergy_start_timeperiod parameter in job collections
        and loads them into this timetable"""
        timeperiod = settings.settings['synergy_start_timeperiod']
        yearly_timeperiod = time_helper.cast_to_time_qualifier(QUALIFIER_YEARLY, timeperiod)
        monthly_timeperiod = time_helper.cast_to_time_qualifier(QUALIFIER_MONTHLY, timeperiod)
        daily_timeperiod = time_helper.cast_to_time_qualifier(QUALIFIER_DAILY, timeperiod)
        hourly_timeperiod = time_helper.cast_to_time_qualifier(QUALIFIER_HOURLY, timeperiod)

        self._build_tree_by_level(COLLECTION_JOB_HOURLY, since=hourly_timeperiod)
        self._build_tree_by_level(COLLECTION_JOB_DAILY, since=daily_timeperiod)
        self._build_tree_by_level(COLLECTION_JOB_MONTHLY, since=monthly_timeperiod)
        self._build_tree_by_level(COLLECTION_JOB_YEARLY, since=yearly_timeperiod)

    @thread_safe
    def build_trees(self):
        """ method iterates thru all trees and ensures that all time-period nodes are created up till <utc_now>"""
        for tree_name, tree in self.trees.iteritems():
            tree.build_tree()

    @thread_safe
    def validate(self):
        """validates that none of nodes in tree is improperly finalized and that every node has job_record"""
        for tree_name, tree in self.trees.iteritems():
            tree.validate()

    @thread_safe
    def is_dependent_on_finalized(self, process_name, job_record):
        """ @return tuple (all_finalized, all_processed, skipped_present) indicating
                all_finalized - True if all <dependent on> periods are in STATE_PROCESSED or STATE_SKIPPED
                all_processed - True if all <dependent on> periods are in STATE_PROCESSED
                skipped_present - True if among <dependent on> periods are some in STATE_SKIPPED
        """
        tree = self.get_tree(process_name)
        node = tree.get_node_by_process(process_name, job_record.timeperiod)
        return node.is_dependent_on_finalized()

    # *** Job manipulation methods ***
    @thread_safe
    def update_job_record(self, process_name, job_record, uow, new_state):
        """ method updates job record with a new unit_of_work and new state"""
        job_record.state = new_state
        job_record.related_unit_of_work = uow.db_id
        job_record.start_id = uow.start_id
        job_record.end_id = uow.end_id
        self.job_dao.update(job_record)

        tree = self.get_tree(process_name)
        tree.update_node_by_process(process_name, job_record)
        self.logger.info('Updated job record %s in timeperiod %s for %s as %s'
                         % (job_record.db_id, job_record.timeperiod, process_name, new_state))

    @thread_safe
    def failed_on_processing_job_record(self, process_name, timeperiod):
        """method increases node's inner counter of failed processing
        if _skip_node logic returns True - node is set to STATE_SKIP"""
        tree = self.get_tree(process_name)
        node = tree.get_node_by_process(process_name, timeperiod)
        node.job_record.number_of_failures += 1
        if tree._skip_the_node(node):
            node.request_skip()
        else:
            # job record is automatically updated in request_skip()
            # so if the node was not skipped - job record has to be updated explicitly
            self.job_dao.update(node.job_record)

    @thread_safe
    def get_next_job_record(self, process_name):
        """returns next job record to work on for given process"""
        if process_name in self.reprocess and len(self.reprocess[process_name]) > 0:
            timeperiod = sorted(self.reprocess[process_name].keys())[0]
            node = self.reprocess[process_name][timeperiod]
            del self.reprocess[process_name][timeperiod]
        else:
            tree = self.get_tree(process_name)
            node = tree.get_next_node_by_process(process_name)

        if node.job_record is None:
            node.request_embryo_job_record()
        return node.job_record

    @thread_safe
    def can_finalize_job_record(self, process_name, job_record):
        """ @return True, if the node and all its children are either in STATE_PROCESSED or STATE_SKIPPED"""
        tree = self.get_tree(process_name)
        node = tree.get_node_by_process(process_name, job_record.timeperiod)
        return node.can_finalize_job_record()

    @thread_safe
    def add_log_entry(self, process_name, job_record, msg_dt, msg):
        """ adds a log entry to the tree node. log entries has no persistence """
        tree = self.get_tree(process_name)
        node = tree.get_node_by_process(process_name, job_record.timeperiod)
        node.add_log_entry([msg_dt.strftime('%Y-%m-%d %H:%M:%S'), msg])
