__author__ = 'Bohdan Mushkevych'


from datetime import datetime
from threading import RLock
from bson.objectid import ObjectId
from model import unit_of_work_helper, time_table, unit_of_work
from model import base_model
from model.time_table import TimeTable
from system.decorator import thread_safe
from tree import TwoLevelTree, ThreeLevelTree, FourLevelTree
from system import process_context
from system.process_context import ProcessContext
from system.collection_context import CollectionContext
from system.collection_context import COLLECTION_TIMETABLE_DAILY, COLLECTION_TIMETABLE_HOURLY, \
    COLLECTION_TIMETABLE_MONTHLY, COLLECTION_TIMETABLE_YEARLY


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

    # *** Timetable collection helper ***
    @thread_safe
    def _get_timetable_collection(self, process_name):
        """timetable stores timeperiod in 4 collections: hourly, daily, monthly and yearly; method looks for the
        proper timetable_collection base on process TIME_QUALIFIER"""
        qualifier = ProcessContext.get_time_qualifier(process_name)
        if qualifier == ProcessContext.QUALIFIER_HOURLY:
            collection = CollectionContext.get_collection(self.logger, COLLECTION_TIMETABLE_HOURLY)
        elif qualifier == ProcessContext.QUALIFIER_DAILY:
            collection = CollectionContext.get_collection(self.logger, COLLECTION_TIMETABLE_DAILY)
        elif qualifier == ProcessContext.QUALIFIER_MONTHLY:
            collection = CollectionContext.get_collection(self.logger, COLLECTION_TIMETABLE_MONTHLY)
        elif qualifier == ProcessContext.QUALIFIER_YEARLY:
            collection = CollectionContext.get_collection(self.logger, COLLECTION_TIMETABLE_YEARLY)
        else:
            raise ValueError('unknown time qualifier: %s for %s' % (qualifier, process_name))
        return collection

    @thread_safe
    def _save_time_record(self, process_name, time_record):
        collection = self._get_timetable_collection(process_name)
        w_number = CollectionContext.get_w_number(self.logger, COLLECTION_TIMETABLE_YEARLY)
        return collection.save(time_record.document, safe=True, w=w_number)

    @thread_safe
    def update_timetable_record(self, process_name, time_record, uow, new_state):
        """ method updates time_record with new unit_of_work and new state"""
        time_record.state = new_state
        time_record.related_unit_of_work = uow.document['_id']
        time_record.start_id = uow.start_id
        time_record.end_id = uow.end_id
        self._save_time_record(process_name, time_record)

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
    def _callback_reprocess(self, process_name, timestamp, tree_node):
        """ is called from tree to answer reprocessing request.
        It is possible that timetable record will be transferred to STATE_IN_PROGRESS with no related unit_of_work"""
        uow_id = tree_node.time_record.related_unit_of_work
        if uow_id is not None:
            tree_node.time_record.state = time_table.STATE_IN_PROGRESS
            uow_obj = unit_of_work_helper.retrieve_by_id(self.logger, ObjectId(uow_id))
            uow_obj.state = unit_of_work.STATE_INVALID
            uow_obj.number_of_retries = 0
            uow_obj.created_at = datetime.utcnow()
            unit_of_work_helper.update(self.logger, uow_obj)
            msg = 'Transferred time-record %s in timeperiod %s to %s; Transferred unit_of_work to %s' \
                  % (tree_node.time_record.document['_id'], tree_node.time_record.timeperiod,
                     tree_node.time_record.state, uow_obj.state)
        else:
            tree_node.time_record.state = time_table.STATE_EMBRYO
            msg = 'Transferred time-record %s in timeperiod %s to %s;' \
                  % (tree_node.time_record.document['_id'], tree_node.time_record.timeperiod,
                     tree_node.time_record.state)

        tree_node.time_record.number_of_failures = 0
        self._save_time_record(process_name, tree_node.time_record)
        self.logger.warning(msg)
        tree_node.add_log_entry([datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), msg])

        if process_name not in self.reprocess:
            self.reprocess[process_name] = dict()
        self.reprocess[process_name][timestamp] = tree_node

    @thread_safe
    def _callback_skip(self, process_name, timestamp, tree_node):
        """ is called from tree to answer skip request"""
        tree_node.time_record.state = time_table.STATE_SKIPPED
        uow_id = tree_node.time_record.related_unit_of_work
        if uow_id is not None:
            uow_obj = unit_of_work_helper.retrieve_by_id(self.logger, ObjectId(uow_id))
            uow_obj.state = uow_obj.STATE_CANCELED
            unit_of_work_helper.update(self.logger, uow_obj)
            msg = 'Transferred time-record %s in timeperiod %s to %s; Transferred unit_of_work to %s' \
                  % (tree_node.time_record.document['_id'], tree_node.time_record.timeperiod,
                     tree_node.time_record.state, uow_obj.state)
        else:
            msg = 'Transferred time-record %s in timeperiod %s to %s;' \
                  % (tree_node.time_record.document['_id'], tree_node.time_record.timeperiod,
                     tree_node.time_record.state)

        self._save_time_record(process_name, tree_node.time_record)
        self.logger.warning(msg)
        tree_node.add_log_entry([datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), msg])

        if process_name in self.reprocess and timestamp in self.reprocess[process_name]:
            del self.reprocess[process_name][timestamp]

    @thread_safe
    def _callback_timetable_record(self, process_name, timeperiod, tree_node):
        """ is called from tree to create timetable record and bind it to the tree node"""
        collection = self._get_timetable_collection(process_name)
        time_record = collection.find_one({time_table.PROCESS_NAME: process_name,
                                           base_model.TIMEPERIOD: timeperiod})

        if time_record is None:
            time_record = TimeTable()
            time_record.state = time_table.STATE_EMBRYO
            time_record.timeperiod = timeperiod
            time_record.process_name = process_name

            tr_id = self._save_time_record(process_name, time_record)
            self.logger.info('Created time-record %s, with timestamp %s for process %s'
                             % (str(tr_id), timeperiod, process_name))
        tree_node.time_record = time_record

    @thread_safe
    def _build_tree_by_level(self, collection):
        """ method iterated thru all documents in all timetable collections and builds tree of known system state"""
        cursor = collection.find({})
        if cursor.count() == 0:
            self.logger.warning('No TimeTable Records in %s.' % str(collection))
        else:
            for document in cursor:
                obj = TimeTable(document)
                tree = self.get_tree(obj.process_name)
                if tree is not None:
                    tree.update_node_by_process(obj.process_name, obj)
                else:
                    self.logger.warning('Skipping TimeTable record for %s, as no tree is handling it.'
                                        % obj.process_name)

    @thread_safe
    def load_tree(self):
        """ method iterates thru all objects in timetable collections and load them into timetable"""
        self._build_tree_by_level(CollectionContext.get_collection(self.logger, COLLECTION_TIMETABLE_HOURLY))
        self._build_tree_by_level(CollectionContext.get_collection(self.logger, COLLECTION_TIMETABLE_DAILY))
        self._build_tree_by_level(CollectionContext.get_collection(self.logger, COLLECTION_TIMETABLE_MONTHLY))
        self._build_tree_by_level(CollectionContext.get_collection(self.logger, COLLECTION_TIMETABLE_YEARLY))

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
    def failed_on_processing_timetable_record(self, process_name, timestamp):
        """method increases node's inner counter of failed processing
        if _skip_node logic returns True - node is set to STATE_SKIP"""
        tree = self.get_tree(process_name)
        node = tree.get_node_by_process(process_name, timestamp)
        node.time_record.number_of_failures += 1
        if tree._skip_the_node(node):
            node.request_skip()
        else:
            # time_record is automatically updated in request_skip()
            # so if node was not skipped - time_record have to be updated explicitly
            self._save_time_record(process_name, node.time_record)

    @thread_safe
    def get_next_timetable_record(self, process_name):
        """returns next time-period to work on for given process"""
        if process_name in self.reprocess and len(self.reprocess[process_name]) > 0:
            timestamp = sorted(self.reprocess[process_name].keys())[0]
            node = self.reprocess[process_name][timestamp]
            del self.reprocess[process_name][timestamp]
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
