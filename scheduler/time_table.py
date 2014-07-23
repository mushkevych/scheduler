__author__ = 'Bohdan Mushkevych'

from datetime import datetime
from threading import RLock

from db.dao.unit_of_work_dao import UnitOfWorkDao
from db.dao.time_table_record_dao import TimeTableRecordDao
from db.model import time_table_record, unit_of_work
from db.model.time_table_record import TimeTableRecord

from settings import settings
from system import process_context, time_helper
from system.process_context import ProcessContext
from system.decorator import thread_safe
from system.collection_context import COLLECTION_TIMETABLE_HOURLY, COLLECTION_TIMETABLE_DAILY, \
    COLLECTION_TIMETABLE_MONTHLY, COLLECTION_TIMETABLE_YEARLY
from scheduler.tree import TwoLevelTree, ThreeLevelTree, FourLevelTree
from scheduler.tree_node import AbstractNode

# make sure MX_PAGE_TRAFFIC refers to mx.views.py page
MX_PAGE_TRAFFIC = 'traffic_details'


class TimeTable(object):
    """ Timetable present a tree, where every node presents a time-period """

    def __init__(self, logger):
        self.lock = RLock()
        self.logger = logger
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.ttr_dao = TimeTableRecordDao(self.logger)
        self.reprocess = dict()

        # self.trees contain all of the trees and manages much of their life cycle
        # remember to enlist there all trees the system is working with
        self.trees = list()

        self.vertical_site = FourLevelTree(process_context.PROCESS_SITE_YEARLY,
                                           process_context.PROCESS_SITE_MONTHLY,
                                           process_context.PROCESS_SITE_DAILY,
                                           process_context.PROCESS_SITE_HOURLY,
                                           process_context._TOKEN_SITE,
                                           MX_PAGE_TRAFFIC)
        self.trees.append(self.vertical_site)

        self.horizontal_client = ThreeLevelTree(process_context.PROCESS_CLIENT_YEARLY,
                                                process_context.PROCESS_CLIENT_MONTHLY,
                                                process_context.PROCESS_CLIENT_DAILY,
                                                process_context._TOKEN_CLIENT,
                                                MX_PAGE_TRAFFIC)
        self.trees.append(self.horizontal_client)

        self.linear_daily_alert = TwoLevelTree(process_context.PROCESS_ALERT_DAILY,
                                               process_context._TOKEN_ALERT,
                                               MX_PAGE_TRAFFIC)
        self.trees.append(self.linear_daily_alert)

        self._register_callbacks()
        self._register_dependencies()
        self.load_tree()
        self.build_tree()
        self.validate()

    def _register_dependencies(self):
        """ register dependencies between trees"""
        #        self.horizontal_client.register_dependent_on(self.vertical_site)
        #        self.horizontal_client.register_dependent_on(self.vertical_visitor)
        #        self.linear_daily_alert.register_dependent_on(self.post_proc_site)
        #        self.linear_daily_alert.register_dependent_on(self.vertical_visitor)
        pass

    def _register_callbacks(self):
        """ register logic that reacts on reprocessing request
        and create embryo timetable record request"""

        # reprocessing request
        for tree in self.trees:
            tree.register_reprocess_callback(self._callback_reprocess)

        # skip request
        for tree in self.trees:
            tree.register_skip_callback(self._callback_skip)

        # callbacks register
        for tree in self.trees:
            tree.register_timetable_callbacks(self._callback_timetable_record)

    @thread_safe
    def update_timetable_record(self, process_name, timetable_record, uow, new_state):
        """ method updates timetable_record with new unit_of_work and new state"""
        timetable_record.state = new_state
        timetable_record.related_unit_of_work = uow.document['_id']
        timetable_record.start_id = uow.start_id
        timetable_record.end_id = uow.end_id
        self.ttr_dao.update(timetable_record)

        tree = self.get_tree(process_name)
        tree.update_node_by_process(process_name, timetable_record)
        self.logger.info('Updated time-table-record %s in timeperiod %s for %s as %s'
                         % (timetable_record.document['_id'],
                            timetable_record.timeperiod,
                            process_name,
                            new_state))

    # *** Regular code ***
    @thread_safe
    def get_tree(self, process_name):
        """ return tree that is managing time-periods for given process"""
        for tree in self.trees:
            if tree.is_managing_process(process_name):
                return tree

    def _find_dependant_trees(self, tree_obj):
        """ returns list of trees that are dependent_on the given tree_obj """
        dependant_trees = []
        for tree in self.trees:
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
        uow_id = tree_node.timetable_record.related_unit_of_work
        if uow_id is not None:
            tree_node.timetable_record.state = time_table_record.STATE_IN_PROGRESS
            uow_obj = self.uow_dao.get_one(uow_id)
            uow_obj.state = unit_of_work.STATE_INVALID
            uow_obj.number_of_retries = 0
            uow_obj.created_at = datetime.utcnow()
            self.uow_dao.update(uow_obj)
            msg = 'Transferred time-table-record %s for %s in timeperiod %s to %s; Transferred unit_of_work to %s' \
                  % (tree_node.timetable_record.document['_id'],
                     tree_node.process_name,
                     tree_node.timetable_record.timeperiod,
                     tree_node.timetable_record.state,
                     uow_obj.state)

            if tree_node.process_name not in self.reprocess:
                self.reprocess[tree_node.process_name] = dict()
            self.reprocess[tree_node.process_name][tree_node.timeperiod] = tree_node

        else:
            tree_node.timetable_record.state = time_table_record.STATE_EMBRYO
            msg = 'Transferred time-table-record %s for %s in timeperiod %s to %s;' \
                  % (tree_node.timetable_record.document['_id'],
                     tree_node.process_name,
                     tree_node.timetable_record.timeperiod,
                     tree_node.timetable_record.state)

        tree_node.timetable_record.number_of_failures = 0
        self.ttr_dao.update(tree_node.timetable_record)
        self.logger.warning(msg)
        tree_node.add_log_entry([datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), msg])

    @thread_safe
    def _callback_reprocess(self, tree_node):
        """ is called from tree to answer reprocessing request.
        It is possible that timetable record will be transferred to STATE_IN_PROGRESS with no related unit_of_work"""
        self._reprocess_single_tree_node(tree_node)

        reprocessing_nodes = self._find_dependant_tree_nodes(tree_node)
        for node in reprocessing_nodes:
            node.request_reprocess()

    @thread_safe
    def _callback_skip(self, tree_node):
        """ is called from tree to answer skip request"""
        tree_node.timetable_record.state = time_table_record.STATE_SKIPPED
        uow_id = tree_node.timetable_record.related_unit_of_work
        if uow_id is not None:
            uow_obj = self.uow_dao.get_one(uow_id)
            uow_obj.state = unit_of_work.STATE_CANCELED
            self.uow_dao.update(uow_obj)
            msg = 'Transferred time-table-record %s in timeperiod %s to %s; Transferred unit_of_work to %s' \
                  % (tree_node.timetable_record.document['_id'],
                     tree_node.timetable_record.timeperiod,
                     tree_node.timetable_record.state,
                     uow_obj.state)
        else:
            msg = 'Transferred time-table-record %s in timeperiod %s to %s;' \
                  % (tree_node.timetable_record.document['_id'],
                     tree_node.timetable_record.timeperiod,
                     tree_node.timetable_record.state)

        self.ttr_dao.update(tree_node.timetable_record)
        self.logger.warning(msg)
        tree_node.add_log_entry([datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), msg])

        if tree_node.process_name in self.reprocess \
                and tree_node.timeperiod in self.reprocess[tree_node.process_name]:
            del self.reprocess[tree_node.process_name][tree_node.timeperiod]

    @thread_safe
    def _callback_timetable_record(self, tree_node):
        """ is called from tree to create timetable record and bind it to the tree node"""

        try:
            timetable_record = self.ttr_dao.get_one(tree_node.process_name, tree_node.timeperiod)
        except LookupError:
            timetable_record = TimeTableRecord()
            timetable_record.state = time_table_record.STATE_EMBRYO
            timetable_record.timeperiod = tree_node.timeperiod
            timetable_record.process_name = tree_node.process_name

            tr_id = self.ttr_dao.update(timetable_record)
            self.logger.info('Created time-table-record %s, with timeperiod %s for process %s'
                             % (str(tr_id), tree_node.timeperiod, tree_node.process_name))
        tree_node.timetable_record = timetable_record

    @thread_safe
    def _build_tree_by_level(self, collection_name, since):
        """ method iterated thru all documents in all timetable collections and builds tree of known system state"""

        try:
            unsupported_records = dict()
            document_list = self.ttr_dao.get_all(collection_name, since)
            for document in document_list:
                tree = self.get_tree(document.process_name)
                if tree is not None:
                    tree.update_node_by_process(document.process_name, document)
                else:
                    unsupported_records[document.process_name] = unsupported_records.get(document.process_name, 0) + 1

            for name, counter in unsupported_records.items():
                self.logger.warning('Skipping %r TimeTable records for %s as no tree is handling it.' % (counter, name))

        except LookupError:
            self.logger.warning('No TimeTable Records in %s.' % str(collection_name))

    @thread_safe
    def load_tree(self):
        """ method iterates thru all objects older than synergy_start_timeperiod parameter in timetable collections
        and load them into timetable"""
        timeperiod = settings['synergy_start_timeperiod']
        yearly_timeperiod = time_helper.cast_to_time_qualifier(ProcessContext.QUALIFIER_YEARLY, timeperiod)
        monthly_timeperiod = time_helper.cast_to_time_qualifier(ProcessContext.QUALIFIER_MONTHLY, timeperiod)
        daily_timeperiod = time_helper.cast_to_time_qualifier(ProcessContext.QUALIFIER_DAILY, timeperiod)
        hourly_timeperiod = time_helper.cast_to_time_qualifier(ProcessContext.QUALIFIER_HOURLY, timeperiod)

        self._build_tree_by_level(COLLECTION_TIMETABLE_HOURLY, since=hourly_timeperiod)
        self._build_tree_by_level(COLLECTION_TIMETABLE_DAILY, since=daily_timeperiod)
        self._build_tree_by_level(COLLECTION_TIMETABLE_MONTHLY, since=monthly_timeperiod)
        self._build_tree_by_level(COLLECTION_TIMETABLE_YEARLY, since=yearly_timeperiod)

    @thread_safe
    def build_tree(self):
        """ method iterates thru all trees and ensures that all time-period nodes are created up till <utc_now>"""
        for tree in self.trees:
            tree.build_tree()

    @thread_safe
    def validate(self):
        """validates that none of nodes in tree is improperly finalized and that every node has timetable_record"""
        for tree in self.trees:
            tree.validate()

    @thread_safe
    def failed_on_processing_timetable_record(self, process_name, timeperiod):
        """method increases node's inner counter of failed processing
        if _skip_node logic returns True - node is set to STATE_SKIP"""
        tree = self.get_tree(process_name)
        node = tree.get_node_by_process(process_name, timeperiod)
        node.timetable_record.number_of_failures += 1
        if tree._skip_the_node(node):
            node.request_skip()
        else:
            # timetable_record is automatically updated in request_skip()
            # so if node was not skipped - timetable_record have to be updated explicitly
            self.ttr_dao.update(node.timetable_record)

    @thread_safe
    def get_next_timetable_record(self, process_name):
        """returns next time-period to work on for given process"""
        if process_name in self.reprocess and len(self.reprocess[process_name]) > 0:
            timeperiod = sorted(self.reprocess[process_name].keys())[0]
            node = self.reprocess[process_name][timeperiod]
            del self.reprocess[process_name][timeperiod]
        else:
            tree = self.get_tree(process_name)
            node = tree.get_next_node_by_process(process_name)

        if node.timetable_record is None:
            node.request_timetable_record()
        return node.timetable_record

    @thread_safe
    def can_finalize_timetable_record(self, process_name, timetable_record):
        """ @return True, if node and all its children are either in STATE_PROCESSED or STATE_SKIPPED"""
        tree = self.get_tree(process_name)
        node = tree.get_node_by_process(process_name, timetable_record.timeperiod)
        return node.can_finalize_timetable_record()

    @thread_safe
    def is_dependent_on_finalized(self, process_name, timetable_record):
        """ @return tuple (all_finalized, all_processed, skipped_present) indicating
                all_finalized - True if all <dependent on> periods are in STATE_PROCESSED or STATE_SKIPPED
                all_processed - True if all <dependent on> periods are in STATE_PROCESSED
                skipped_present - True if among <dependent on> periods are some in STATE_SKIPPED
        """
        tree = self.get_tree(process_name)
        node = tree.get_node_by_process(process_name, timetable_record.timeperiod)
        return node.is_dependent_on_finalized()

    @thread_safe
    def add_log_entry(self, process_name, timetable_record, msg_dt, msg):
        """ adds a log entry to the tree node. log entries has no persistence """
        tree = self.get_tree(process_name)
        node = tree.get_node_by_process(process_name, timetable_record.timeperiod)
        node.add_log_entry([msg_dt.strftime('%Y-%m-%d %H:%M:%S'), msg])
