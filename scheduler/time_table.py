__author__ = 'Bohdan Mushkevych'

from datetime import datetime
from threading import RLock
from model import unit_of_work_dao, time_table_record, unit_of_work, time_table_record_dao
from model.time_table_record import TimeTableRecord
from system.decorator import thread_safe
from system.collection_context import COLLECTION_TIMETABLE_HOURLY, COLLECTION_TIMETABLE_DAILY, \
    COLLECTION_TIMETABLE_MONTHLY, COLLECTION_TIMETABLE_YEARLY
from tree import TwoLevelTree, ThreeLevelTree, FourLevelTree
from system import process_context

# make sure MX_PAGE_TRAFFIC refers to mx.views.py page
MX_PAGE_TRAFFIC = 'traffic_details'


class TimeTable:
    """ Timetable present a tree, where every node presents a time-period """

    def __init__(self, logger):
        self.lock = RLock()
        self.logger = logger
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
        self._register_dependents()
        self.load_tree()
        self.build_tree()
        self.validate()

    def _register_dependents(self):
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
    def update_timetable_record(self, process_name, time_record, uow, new_state):
        """ method updates time_record with new unit_of_work and new state"""
        time_record.state = new_state
        time_record.related_unit_of_work = uow.document['_id']
        time_record.start_id = uow.start_id
        time_record.end_id = uow.end_id
        time_table_record_dao.update(self.logger, time_record)

        tree = self.get_tree(process_name)
        tree.update_node_by_process(process_name, time_record)
        self.logger.info('Updated time-record %s in timeperiod %s for %s as %s'
                         % (time_record.document['_id'], time_record.timeperiod, process_name, new_state))

    # *** Regular code ***
    @thread_safe
    def get_tree(self, process_name):
        """ return tree that is managing time-periods for given process"""
        for tree in self.trees:
            if tree.is_managing_process(process_name):
                return tree

    @thread_safe
    def _callback_reprocess(self, process_name, timeperiod, tree_node):
        """ is called from tree to answer reprocessing request.
        It is possible that timetable record will be transferred to STATE_IN_PROGRESS with no related unit_of_work"""
        uow_id = tree_node.time_record.related_unit_of_work
        if uow_id is not None:
            tree_node.time_record.state = time_table_record.STATE_IN_PROGRESS
            uow_obj = unit_of_work_dao.get_one(self.logger, uow_id)
            uow_obj.state = unit_of_work.STATE_INVALID
            uow_obj.number_of_retries = 0
            uow_obj.created_at = datetime.utcnow()
            unit_of_work_dao.update(self.logger, uow_obj)
            msg = 'Transferred time-record %s in timeperiod %s to %s; Transferred unit_of_work to %s' \
                  % (tree_node.time_record.document['_id'], tree_node.time_record.timeperiod,
                     tree_node.time_record.state, uow_obj.state)
        else:
            tree_node.time_record.state = time_table_record.STATE_EMBRYO
            msg = 'Transferred time-record %s in timeperiod %s to %s;' \
                  % (tree_node.time_record.document['_id'], tree_node.time_record.timeperiod,
                     tree_node.time_record.state)

        tree_node.time_record.number_of_failures = 0
        time_table_record_dao.update(self.logger, tree_node.time_record)
        self.logger.warning(msg)
        tree_node.add_log_entry([datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), msg])

        if process_name not in self.reprocess:
            self.reprocess[process_name] = dict()
        self.reprocess[process_name][timeperiod] = tree_node

    @thread_safe
    def _callback_skip(self, process_name, timeperiod, tree_node):
        """ is called from tree to answer skip request"""
        tree_node.time_record.state = time_table_record.STATE_SKIPPED
        uow_id = tree_node.time_record.related_unit_of_work
        if uow_id is not None:
            uow_obj = unit_of_work_dao.get_one(self.logger, uow_id)
            uow_obj.state = uow_obj.STATE_CANCELED
            unit_of_work_dao.update(self.logger, uow_obj)
            msg = 'Transferred time-record %s in timeperiod %s to %s; Transferred unit_of_work to %s' \
                  % (tree_node.time_record.document['_id'], tree_node.time_record.timeperiod,
                     tree_node.time_record.state, uow_obj.state)
        else:
            msg = 'Transferred time-record %s in timeperiod %s to %s;' \
                  % (tree_node.time_record.document['_id'], tree_node.time_record.timeperiod,
                     tree_node.time_record.state)

        time_table_record_dao.update(self.logger, tree_node.time_record)
        self.logger.warning(msg)
        tree_node.add_log_entry([datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), msg])

        if process_name in self.reprocess and timeperiod in self.reprocess[process_name]:
            del self.reprocess[process_name][timeperiod]

    @thread_safe
    def _callback_timetable_record(self, process_name, timeperiod, tree_node):
        """ is called from tree to create timetable record and bind it to the tree node"""

        try:
            time_record = time_table_record_dao.get_one(self.logger, property, timeperiod)
        except LookupError:
            time_record = TimeTableRecord()
            time_record.state = time_table_record.STATE_EMBRYO
            time_record.timeperiod = timeperiod
            time_record.process_name = process_name

            tr_id = time_table_record_dao.update(self.logger, time_record)
            self.logger.info('Created time-record %s, with timeperiod %s for process %s'
                             % (str(tr_id), timeperiod, process_name))
        tree_node.time_record = time_record

    @thread_safe
    def _build_tree_by_level(self, collection_name):
        """ method iterated thru all documents in all timetable collections and builds tree of known system state"""

        try:
            document_list = time_table_record_dao.get_all(self.logger, collection_name)
            for document in document_list:
                tree = self.get_tree(document.process_name)
                if tree is not None:
                    tree.update_node_by_process(document.process_name, document)
                else:
                    self.logger.warning('Skipping TimeTable record for %s, as no tree is handling it.'
                                        % document.process_name)
        except LookupError:
            self.logger.warning('No TimeTable Records in %s.' % str(collection_name))

    @thread_safe
    def load_tree(self):
        """ method iterates thru all objects in timetable collections and load them into timetable"""
        self._build_tree_by_level(COLLECTION_TIMETABLE_HOURLY)
        self._build_tree_by_level(COLLECTION_TIMETABLE_DAILY)
        self._build_tree_by_level(COLLECTION_TIMETABLE_MONTHLY)
        self._build_tree_by_level(COLLECTION_TIMETABLE_YEARLY)

    @thread_safe
    def build_tree(self):
        """ method iterates thru all trees and ensures that all time-period nodes are created up till <utc_now>"""
        for tree in self.trees:
            tree.build_tree()

    @thread_safe
    def validate(self):
        """validates that none of nodes in tree is improperly finalized and that every node has time_record"""
        for tree in self.trees:
            tree.validate()

    @thread_safe
    def failed_on_processing_timetable_record(self, process_name, timeperiod):
        """method increases node's inner counter of failed processing
        if _skip_node logic returns True - node is set to STATE_SKIP"""
        tree = self.get_tree(process_name)
        node = tree.get_node_by_process(process_name, timeperiod)
        node.time_record.number_of_failures += 1
        if tree._skip_the_node(node):
            node.request_skip()
        else:
            # time_record is automatically updated in request_skip()
            # so if node was not skipped - time_record have to be updated explicitly
            time_table_record_dao.update(self.logger, node.time_record)

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

        if node.time_record is None:
            node.request_timetable_record()
        return node.time_record

    @thread_safe
    def can_finalize_timetable_record(self, process_name, time_record):
        """ @return True, if node and all its children are either in STATE_PROCESSED or STATE_SKIPPED"""
        tree = self.get_tree(process_name)
        node = tree.get_node_by_process(process_name, time_record.timeperiod)
        return node.can_finalize_timetable_record()

    @thread_safe
    def is_dependent_on_finalized(self, process_name, time_record):
        """ @return tuple (dependents_are_finalized, dependents_are_skipped) indicating
                dependents_are_finalized - indicates if all <dependent on> periods are in STATE_PROCESSED
                dependents_are_skipped - indicates that among <dependent on> periods are some in STATE_SKIPPED
        """
        tree = self.get_tree(process_name)
        node = tree.get_node_by_process(process_name, time_record.timeperiod)
        return node.is_dependent_on_finalized()

    @thread_safe
    def add_log_entry(self, process_name, time_record, msg_dt, msg):
        """ adds a log entry to the tree node. log entries has no persistence """
        tree = self.get_tree(process_name)
        node = tree.get_node_by_process(process_name, time_record.timeperiod)
        node.add_log_entry([msg_dt.strftime('%Y-%m-%d %H:%M:%S'), msg])
