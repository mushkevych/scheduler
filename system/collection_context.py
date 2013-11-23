"""
Created on 2011-04-23

@author: Bohdan Mushkevych
@author: Aaron Westendorf
"""
import time
from settings import settings
from pymongo.connection import Connection as MongoConnection
from pymongo.uri_parser import split_hosts, parse_host
from pymongo.master_slave_connection import MasterSlaveConnection

COLLECTION_SINGLE_SESSION = 'single_session'
COLLECTION_SCHEDULER_CONFIGURATION = 'scheduler_configuration'
COLLECTION_UNITS_OF_WORK = 'units_of_work'
COLLECTION_BOX_CONFIGURATION = 'box_configuration'

COLLECTION_TIMETABLE_HOURLY = 'timetable_hourly'
COLLECTION_TIMETABLE_DAILY = 'timetable_daily'
COLLECTION_TIMETABLE_MONTHLY = 'timetable_monthly'
COLLECTION_TIMETABLE_YEARLY = 'timetable_yearly'

REPLICA_SET_SSC = 'replica_set_sss'
REPLICA_SET_SYSTEM = 'replica_set_system'


class ClusterConnection(MasterSlaveConnection):
    """ - UTC friendly
        - redirects all reads to ReplicaSet slaves
        - all writes go to ReplicaSet master
        - re-connect to lost slaves node from ReplicaSet every 5 min
        - automatic handling of AutoReconnect or Master change
    """

    VALIDATE_INTERVAL = 300 # 5 minutes

    def __init__(self, logger, host_list):
        """@param host_list: initial list of nodes in ReplicaSet (can change during the life time)"""
        self.logger = logger
        self.host_list = host_list
        master_connection = MongoConnection(self.host_list)

        slave_log_list = []
        slave_connections = []
        for host in self.host_list:
            slave_host, slave_port = split_hosts(host)

            # remove master from list of slaves, so no reads are going its way
            # however, allow master to handle reads if its the only node in ReplicaSet
            if len(self.host_list) > 1 \
                and slave_host == master_connection._Connection__host \
                and slave_port == master_connection._Connection__port:
                continue
            slave_log_list.append('%s:%r' % (slave_host, slave_port))
            slave_connections.append(MongoConnection(host=slave_host, port=slave_port, slave_okay=True, _connect=False))
        self.logger.info('ClusterConnection.init: master %r, slaves: %r' % (master_connection,  slave_log_list))

        super(ClusterConnection, self).__init__(master=master_connection, slaves=slave_connections)
        self._last_validate_time = time.time()

    @property
    def tz_aware(self):
        """ True stands for local-aware timezone, False for UTC """
        return False

    def get_w_number(self):
        """ number of nodes to replicate highly_important data on insert/update """

        w_number = 1
        master_host_port = (self.master._Connection__host, self.master._Connection__port)

        # For each connection that is not master - increase w_number 
        for slave in self.slaves:
            host_port = (slave._Connection__host, slave._Connection__port)
            if host_port == master_host_port:
                continue
            if host_port == (None, None):
                continue
            else:
                w_number += 1

        return w_number

    def validate_slaves(self):
        """
        1. If we're at the check interval, confirm that all slaves are connected to their
        intended hosts and if not, reconnect them.
        2. Remove master from list of slaves.
        """
        if time.time() - self._last_validate_time < self.VALIDATE_INTERVAL:
            return

        master_host_port = (self.master._Connection__host, self.master._Connection__port)
        hosts_ports = [parse_host(uri) for uri in self.host_list]

        # For each connection that is not pointing to a configured slave:
        # - disconnect it and remove from the list.
        for slave in self.slaves:
            host_port = (slave._Connection__host, slave._Connection__port)
            if host_port == master_host_port:
                # use case: master connection is among slaves
                if len(self.slaves) > 1:
                    # remove master from list of slaves, so no reads are going its way
                    # however, allow master to handle reads if its the only node in ReplicaSet
                    slave.disconnect()
                    self.slaves.remove(slave)
                    hosts_ports.remove(master_host_port)
            elif host_port not in hosts_ports:
                slave.disconnect()
                self.slaves.remove(slave)
            else:
                hosts_ports.remove(host_port)

        # use case: remove master URI from "re-connection" list, if there are other active slave connections
        if len(self.slaves) > 0 and master_host_port in hosts_ports:
            # if at least one slave is active - do not try to (re)connect to master
            hosts_ports.remove(master_host_port)

        # For all hosts where there wasn't an existing connection, create one
        for host, port in hosts_ports:
            self.slaves.append(MongoConnection(host=host, port=port, slave_okay=True, _connect=False))

        self.logger.info('ClusterConnection.validate: master %r, slaves: %r' % (self.master,  self.slaves))
        self._last_validate_time = time.time()

    def get_master_host_port(self):
        """ @return current host and port of the master node in Replica Set"""
        return self.master._Connection__host, self.master._Connection__port


class ReplicaSetContext:
    _DB_HOST_LIST = '_db_host_list'

    connection_pool = dict()

    REPLICA_SET_CONTEXT = {
        REPLICA_SET_SSC: {_DB_HOST_LIST: settings['rs_ssc_host_list']},
        REPLICA_SET_SYSTEM: {_DB_HOST_LIST: settings['rs_system_host_list']},
    }

    @classmethod
    def get_connection(cls, logger, replica_set):
        """ method creates ClusterConnection to replica set and returns it"""
        record = cls.REPLICA_SET_CONTEXT[replica_set]
        if replica_set not in cls.connection_pool:
            host_list = record[cls._DB_HOST_LIST]
            cls.connection_pool[replica_set] = ClusterConnection(logger, host_list)
        else:
            cls.connection_pool[replica_set].validate_slaves()
        return cls.connection_pool[replica_set]

class CollectionContext:
    _REPLICA_SET = 'replica_set'
    _RETENTION_PERIOD = 'retention_period' # days
    _OWNER_PROCESS = 'owner_process'

    COLLECTION_CONTEXT = {
        COLLECTION_SINGLE_SESSION :    { _REPLICA_SET : REPLICA_SET_SSC,
                                         _RETENTION_PERIOD : 2,
                                         _OWNER_PROCESS : 'SingleSessionWorker_00'},
        COLLECTION_SCHEDULER_CONFIGURATION : { _REPLICA_SET : REPLICA_SET_SYSTEM},
        COLLECTION_UNITS_OF_WORK :     { _REPLICA_SET : REPLICA_SET_SYSTEM},
        COLLECTION_BOX_CONFIGURATION : { _REPLICA_SET : REPLICA_SET_SYSTEM},

        COLLECTION_TIMETABLE_HOURLY :  { _REPLICA_SET : REPLICA_SET_SYSTEM},
        COLLECTION_TIMETABLE_DAILY :   { _REPLICA_SET : REPLICA_SET_SYSTEM},
        COLLECTION_TIMETABLE_MONTHLY : { _REPLICA_SET : REPLICA_SET_SYSTEM},
        COLLECTION_TIMETABLE_YEARLY :  { _REPLICA_SET : REPLICA_SET_SYSTEM}
    }

    @classmethod
    def get_fixed_connection(cls, logger, collection_name, slave_ok=True):
        """ Method retrieves non-balancing connection from ReplicaSetContext.
         Such connection is locked to one slave node, and will not handle its unavailability.
         Returns fully specified connection to collection."""
        try:
            rs = cls.COLLECTION_CONTEXT[collection_name][cls._REPLICA_SET]
            rs_connection = ReplicaSetContext.get_connection(logger, rs)

            fixed_connection = None
            if slave_ok:
                # case A: client requests slave-tolerant connection
                for slave in rs_connection.slaves:
                    host_port = (slave._Connection__host, slave._Connection__port)
                    if host_port == (None, None):
                        continue
                    else:
                        fixed_connection = slave

            if not slave_ok or fixed_connection is None:
                # case B: ReplicaSet has no valid slave connection, or master connection was requested
                fixed_connection = rs_connection.master
                
            synergy = fixed_connection[settings['mongo_db_name']]
            return synergy[collection_name]
        except Exception:
            logger.error('CollectionContext error: %r' % collection_name, exc_info=True)

    @classmethod
    def get_collection(cls, logger, collection_name):
        """ method retrieves connection from ReplicaSetContext and
        links it to the collection name. Returns fully specified connection to collection.
        Avoid pooling at this level, as it blocks ClusterConnection load balancing"""
        try:
            rs = cls.COLLECTION_CONTEXT[collection_name][cls._REPLICA_SET]
            db_connection = ReplicaSetContext.get_connection(logger, rs)
            synergy = db_connection[settings['mongo_db_name']]
            return synergy[collection_name]
        except Exception:
            logger.error('CollectionContext error: %s' % collection_name, exc_info=True)

    @classmethod
    def get_retention_period(cls, logger, collection_name):
        """ method retrieves duration of the data retention. period is identifies in days.
        if None is returned - data retention policies are inactive for given collection"""
        try:
            context = cls.COLLECTION_CONTEXT[collection_name]
            return context.get(cls._RETENTION_PERIOD)
        except Exception:
            logger.error('CollectionContext error: %s' % collection_name, exc_info=True)

    @classmethod
    def get_owner_process(cls, logger, collection_name):
        """ method retrieves owner process name. Owner process is primary "writer" to the collection"""
        try:
            context = cls.COLLECTION_CONTEXT[collection_name]
            return context.get(cls._OWNER_PROCESS)
        except Exception:
            logger.error('CollectionContext error: %s' % collection_name, exc_info=True)

    @classmethod
    def get_w_number(cls, logger, collection_name):
        """ w number indicates number of nodes to replicate _highly_important_ data on insert/update
        replication of write shall be used only for System collections """
        try:
            rs = cls.COLLECTION_CONTEXT[collection_name][cls._REPLICA_SET]
            db_connection = ReplicaSetContext.get_connection(logger, rs)
            return db_connection.get_w_number()
        except Exception:
            logger.error('CollectionContext error: %s' % collection_name, exc_info=True)

    @classmethod
    def get_master_host_port(cls, logger, collection_name):
        """ @return current host and port of the master node in Replica Set"""
        try:
            rs = cls.COLLECTION_CONTEXT[collection_name][cls._REPLICA_SET]
            db_connection = ReplicaSetContext.get_connection(logger, rs)
            return db_connection.get_master_host_port()
        except Exception:
            logger.error('CollectionContext error: %s' % collection_name, exc_info=True)
