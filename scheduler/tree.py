__author__ = 'Bohdan Mushkevych'


from datetime import datetime, timedelta
from tree_node import TreeNode, LinearNode
from settings import settings
from model import time_table
from system import time_helper
from system.time_helper import cast_to_time_qualifier

MAX_NUMBER_OF_RETRIES = 3    # number of times a node is re-run before it is considered STATE_SKIPPED
LIFE_SUPPORT_HOURS = 48      # number of hours that node is retried infinite number of times


class AbstractTree(object):
    """Linear timeline structure, presenting array of timetable_records"""

    def __init__(self, node_klass, category=None, mx_page=None):
        """
        @parameter node_klass: presents descendant of the AbstractNode class, that is used to instantiate nodes of the tree
        @optional @parameter category: is used by MX only as visual vertical name
        @optional @parameter mx_page: is used by MX only as anchor to specific page
        """
        self.build_timestamp = None
        self.validation_timestamp = None
        self.reprocess_callbacks = []
        self.skip_callbacks = []
        self.create_timetable_record_callbacks = []
        self.category = category
        self.mx_page = mx_page
        self.node_klass = node_klass
        self.root = node_klass(self, None, None, None, None)
        self.dependent_on = []

    # *** PUBLIC METHODS ***
    def register_dependent_on(self, tree):
        """registering tree that we are dependent on.
        example: horizontal client should not be finalized until we have finalized vertical site for the same period"""
        self.dependent_on.append(tree)

    def unregister_dependent_on(self, tree):
        """unregistering tree that we are dependent on"""
        if tree in self.dependent_on:
            self.dependent_on.remove(tree)

    def register_reprocess_callback(self, function):
        """method that allows outside functionality to listen for _reprocess_requests_"""
        self.reprocess_callbacks.append(function)

    def unregister_reprocess_callback(self, function):
        """method that allows outside functionality to abandon _reprocess_requests_ listening"""
        if function in self.reprocess_callbacks:
            self.reprocess_callbacks.remove(function)

    def register_skip_callback(self, function):
        """method that allows outside functionality to listen for _skip_requests_"""
        self.skip_callbacks.append(function)

    def unregister_skip_callback(self, function):
        """method that allows outside functionality to abandon _skip_requests_ listening"""
        if function in self.skip_callbacks:
            self.skip_callbacks.remove(function)

    def register_timetable_callbacks(self, function):
        """method that allows outside functionality to listen for _create_embryo_timetable_record_requests_"""
        self.create_timetable_record_callbacks.append(function)

    def unregister_timetable_callback(self, function):
        """method that allows outside functionality to abandon _create_embryo_timetable_record_requests_ listening"""
        if function in self.create_timetable_record_callbacks:
            self.create_timetable_record_callbacks.remove(function)

    # *** PROTECTED METHODS ***
    def _build_tree(self, rebuild, process_name, method_get_node):
        """method builds tree by iterating from the synergy_start_timestamp to current time
        and inserting corresponding nodes"""

        if rebuild or self.build_timestamp is None:
            timestamp = settings['synergy_start_timestamp']
            timestamp = cast_to_time_qualifier(process_name, timestamp)
        else:
            timestamp = self.build_timestamp

        now = time_helper.datetime_to_synergy(process_name, datetime.utcnow())
        while now >= timestamp:
            method_get_node(timestamp)
            timestamp = time_helper.increment_time(process_name, timestamp)

        self.build_timestamp = now

    def _get_next_parent_node(self, parent):
        """ Used by _get_next_node, this method is called to find next possible parent.
        For example if timeperiod 2011010200 has all children processed, but is not processed yet
        then it makes sense to look in 2011010300 for hourly nodes"""
        parent_of_parent = parent.parent
        if parent_of_parent is None:
            # here, we work at yearly/linear level
            return None

        sorted_keys = sorted(parent_of_parent.children.keys())
        index = sorted_keys.index(parent.timestamp)
        if index + 1 >= len(sorted_keys):
            return None
        else:
            return parent_of_parent.children[sorted_keys[index + 1]]

    def _get_next_node(self, parent):
        """ Looks for next node to process """
        sorted_keys = sorted(parent.children.keys())
        for key in sorted_keys:
            node = parent.children[key]
            if node.time_record is None:
                node.request_timetable_record()
                return node
            elif self._skip_the_node(node):
                continue
            elif node.time_record.state == time_table.STATE_FINAL_RUN \
                    or node.time_record.state == time_table.STATE_IN_PROGRESS \
                    or node.time_record.state == time_table.STATE_EMBRYO:
                return node

        # special case, when all children of the parent node are not suitable for processing
        new_parent = self._get_next_parent_node(parent)
        if new_parent is not None:
            # in case all nodes are processed or blocked - look for next valid parent node
            return self._get_next_node(new_parent)
        else:
            # in all valid parents are exploited - return current node
            process_name = parent.children[sorted_keys[0]].process_name
            timestamp_now = time_helper.datetime_to_synergy(process_name, datetime.utcnow())
            return self.get_node_by_process(process_name, timestamp_now)

    # *** INHERITANCE INTERFACE ***
    def build_tree(self, rebuild=False):
        """method builds tree by iterating from the synergy_start_timestamp to current time
        and inserting corresponding nodes"""
        pass

    def _skip_the_node(self, node):
        """Method is used during _get_next_node calculations.
        Returns True in case node shall be _skipped_"""
        pass

    def is_managing_process(self, process_name):
        """method returns True if process_name is registered on Timeline during creation"""
        pass

    def get_next_node_by_process(self, process_name):
        """ method is used to keep consistency with Three/FourLevelTree interface"""
        pass

    def update_node_by_process(self, process_name, time_record):
        """ method is used to keep consistency with Three/FourLevelTree interface"""
        pass

    def get_node_by_process(self, process_name, timestamp):
        """ method is used to keep consistency with Three/FourLevelTree interface"""
        pass

    def validate(self):
        """method starts validation of the tree.
        @see AbstractNode.validate"""
        for timestamp in self.root.children:
            child = self.root.children[timestamp]
            child.validate()
        self.validation_dt = datetime.utcnow()


class TwoLevelTree(AbstractTree):
    """Linear timeline structure, presenting array of timetable_records"""
    def __init__(self, process_name, category=None, mx_page=None):
        super(TwoLevelTree, self).__init__(LinearNode, category, mx_page)
        self.process_name = process_name

    # *** SPECIFIC METHODS ***
    def __get_node(self, timestamp):
        node = self.root.children.get(timestamp)
        if node is None:
            node = LinearNode(self, self.root, self.process_name, timestamp, None)
            node.request_timetable_record()
            self.root.children[timestamp] = node

        return node

    def __update_node(self, time_record):
        node = self.__get_node(time_record.timeperiod)
        node.time_record = time_record

    # *** INHERITANCE INTERFACE ***
    def build_tree(self, rebuild=False):
        """method builds timeline by iterating from the synergy_start_timestamp to current time
        and inserting nodes"""
        self._build_tree(rebuild, self.process_name, self.__get_node)

    def _skip_the_node(self, node):
        """Method is used during _get_next_node calculations.
        Returns True in case node shall be _skipped_"""
        if node.time_record.state == time_table.STATE_SKIPPED \
                or node.time_record.state == time_table.STATE_PROCESSED:
            return True
        return node.time_record.number_of_failures > MAX_NUMBER_OF_RETRIES

    def is_managing_process(self, process_name):
        """method returns True if process_name is registered on Timeline during creation"""
        if process_name == self.process_name:
            return True
        return False

    def get_next_node_by_process(self, process_name):
        """ method is used to keep consistency with Three/FourLevelTree interface"""
        if process_name == self.process_name:
            return self._get_next_node(self.root)
        else:
            raise ValueError('unknown requested process: %s vs %s' % (process_name, self.process_name))

    def update_node_by_process(self, process_name, time_record):
        """ method is used to keep consistency with Three/FourLevelTree interface"""
        if process_name == self.process_name:
            return self.__update_node(time_record)
        else:
            raise ValueError('unknown requested process: %s vs %s' % (process_name, self.process_name))

    def get_node_by_process(self, process_name, timestamp):
        """ method is used to keep consistency with Three/FourLevelTree interface"""
        if process_name == self.process_name:
            return self.__get_node(timestamp)
        else:
            raise ValueError('unknown requested process: %s vs %s' % (process_name, self.process_name))


class ThreeLevelTree(AbstractTree):
    """Three level tree present structure, monitoring: yearly, monthly and daily time-periods"""
    def __init__(self, process_yearly, process_monthly, process_daily, category=None, mx_page=None):
        super(ThreeLevelTree, self).__init__(TreeNode, category, mx_page)
        self.process_yearly = process_yearly
        self.process_monthly = process_monthly
        self.process_daily = process_daily

    # *** PRIVATE METHODS TO BUILD AND OPERATE TREE ***
    def __get_yearly_node(self, timestamp):
        node = self.root.children.get(timestamp)
        if node is None:
            node = TreeNode(self, self.root, self.process_yearly, timestamp, None)
            self.root.children[timestamp] = node

        return node

    def __get_monthly_node(self, timestamp):
        timestamp_yearly = cast_to_time_qualifier(self.process_yearly, timestamp)
        parent = self.__get_yearly_node(timestamp_yearly)

        node = parent.children.get(timestamp)
        if node is None:
            node = TreeNode(self, parent, self.process_monthly, timestamp, None)
            parent.children[timestamp] = node

        return node

    def __get_daily_node(self, timestamp):
        timestamp_monthly = cast_to_time_qualifier(self.process_monthly, timestamp)
        parent = self.__get_monthly_node(timestamp_monthly)

        node = parent.children.get(timestamp)
        if node is None:
            node = TreeNode(self, parent, self.process_daily, timestamp, None)
            parent.children[timestamp] = node

        return node

    def __get_next_yearly_node(self):
        parent = self.root
        return self._get_next_node(parent)

    def __get_next_monthly_node(self):
        parent = self.__get_next_yearly_node()
        return self._get_next_node(parent)

    def __get_next_daily_node(self):
        parent = self.__get_next_monthly_node()
        return self._get_next_node(parent)

    # *** INHERITANCE INTERFACE ***
    def build_tree(self, rebuild=False):
        """method builds tree by iterating from the synergy_start_timestamp to current time
        and inserting corresponding nodes"""
        self._build_tree(rebuild, self.process_daily, self.__get_daily_node)

    def get_node_by_process(self, process_name, timestamp):
        if process_name == self.process_yearly:
            return self.__get_yearly_node(timestamp)
        if process_name == self.process_monthly:
            return self.__get_monthly_node(timestamp)
        if process_name == self.process_daily:
            return self.__get_daily_node(timestamp)

    def update_node_by_process(self, process_name, time_record):
        if process_name == self.process_yearly:
            node = self.__get_yearly_node(time_record.get_timeperiod())
        elif process_name == self.process_monthly:
            node = self.__get_monthly_node(time_record.get_timeperiod())
        elif process_name == self.process_daily:
            node = self.__get_daily_node(time_record.get_timeperiod())
        else:
            raise ValueError('unknown process name: %s' % process_name)
        node.time_record = time_record

    def _skip_the_node(self, node):
        """Method is used during _get_next_node calculations.
        Returns True in case node shall be _skipped_"""
        # case 1: node processing is complete
        if node.time_record.state == time_table.STATE_SKIPPED \
                or node.time_record.state == time_table.STATE_PROCESSED:
            return True

        # case 2: this is a daily leaf node. retry this time_period for INFINITE_RETRY_HOURS
        if node.process_name == self.process_daily:
            if len(node.children) == 0:
                # no children - this is a leaf
                creation_time = time_helper.synergy_to_datetime(node.process_name, node.timestamp)
                if datetime.utcnow() - creation_time < timedelta(hours=LIFE_SUPPORT_HOURS):
                    return False
                else:
                    return node.time_record.number_of_failures > MAX_NUMBER_OF_RETRIES

        # case 3: here we process process_daily, process_monthly and process_yearly that have children
        # iterate thru children and check if all of them are in STATE_SKIPPED (i.e. no data for parent to process)
        # if any is still in processing (i.e. has produced some data) - then we can not skip parent of the child node
        # case 3': consider parent as worth processing (i.e. do not skip) if child's time_record is None
        all_children_spoiled = True
        for key in node.children.keys():
            child = node.children[key]
            if child.time_record is None or \
                (child.time_record.number_of_failures <= MAX_NUMBER_OF_RETRIES
                    and child.time_record.state != time_table.STATE_SKIPPED):
                all_children_spoiled = False
                break
        return all_children_spoiled

    def get_next_node_by_process(self, process_name):
        if process_name == self.process_yearly:
            return self.__get_next_yearly_node()
        if process_name == self.process_monthly:
            return self.__get_next_monthly_node()
        if process_name == self.process_daily:
            return self.__get_next_daily_node()

    def is_managing_process(self, process_name):
        """method returns True if process_name is among processes (yearly/monthly/daily etc),
        registered on Tree creation"""
        if process_name == self.process_yearly \
                or process_name == self.process_monthly \
                or process_name == self.process_daily:
            return True
        return False


class FourLevelTree(ThreeLevelTree):
    """Four level tree present structure, monitoring: yearly, monthly, daily and hourly time-periods"""
    def __init__(self, process_yearly, process_monthly, process_daily, process_hourly, category=None, mx_page=None):
        super(FourLevelTree, self).__init__(process_yearly, process_monthly, process_daily, category, mx_page)
        self.process_hourly = process_hourly

    # *** PRIVATE METHODS ***
    def __get_hourly_node(self, timestamp):
        timestamp_daily = cast_to_time_qualifier(self.process_daily, timestamp)
        parent = self._ThreeLevelTree__get_daily_node(timestamp_daily)

        node = parent.children.get(timestamp)
        if node is None:
            node = TreeNode(self, parent, self.process_hourly, timestamp, None)
            parent.children[timestamp] = node

        return node

    def __get_next_hourly_node(self):
        parent = self._ThreeLevelTree__get_next_daily_node()
        return self._get_next_node(parent)

    # *** INHERITANCE INTERFACE ***
    def build_tree(self, rebuild=False):
        """@see ThreeLevelTree.build_tree """
        self._build_tree(rebuild, self.process_hourly, self.__get_hourly_node)

    def is_managing_process(self, process_name):
        """@see ThreeLevelTree.is_managing_process"""
        if process_name == self.process_hourly:
            return True
        else:
            return super(FourLevelTree, self).is_managing_process(process_name)

    def get_next_node_by_process(self, process_name):
        if process_name == self.process_hourly:
            return self.__get_next_hourly_node()
        else:
            return super(FourLevelTree, self).get_next_node_by_process(process_name)

    def get_node_by_process(self, process_name, timestamp):
        if process_name == self.process_hourly:
            return self.__get_hourly_node(timestamp)
        else:
            return super(FourLevelTree, self).get_node_by_process(process_name, timestamp)

    def update_node_by_process(self, process_name, time_record):
        if process_name == self.process_hourly:
            node = self.__get_hourly_node(time_record.get_timeperiod())
            node.time_record = time_record
        else:
            return super(FourLevelTree, self).update_node_by_process(process_name, time_record)

    def _skip_the_node(self, node):
        """Method is used during _get_next_node calculations.
        Returns True in case node shall be _skipped_"""
        if node.process_name == self.process_hourly:
            if node.time_record.state == time_table.STATE_SKIPPED \
                    or node.time_record.state == time_table.STATE_PROCESSED:
                return True
            return node.time_record.number_of_failures > MAX_NUMBER_OF_RETRIES
        else:
            return super(FourLevelTree, self)._skip_the_node(node)
