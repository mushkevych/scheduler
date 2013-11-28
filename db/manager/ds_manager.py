__author__ = 'Bohdan Mushkevych'

from pymongo import MongoClient, ASCENDING, DESCENDING
from settings import settings
from db.model import base_model
from abc import abstractmethod, ABCMeta

QUERY_GET_ALL = {}

if 'ds_factory' not in globals():
    # this block defines global variable ds_factory

    def factory():
        # the only way to implement nonlocal closure variables in Python 2.X
        instances = {}

        def get_instance(logger):
            ds_type = settings['ds_type']

            if ds_type not in instances:
                if ds_type == "mongo_db":
                    instances[ds_type] = MongoDbManager(logger)
                elif ds_type == "hbase":
                    instances[ds_type] = HBaseManager(logger)
                else:
                    raise ValueError('Unsupported Data Source type: %s' % ds_type)
            return instances[ds_type]

        return get_instance

    global ds_factory
    ds_factory = factory()


class BaseManager:
    """
    AbstractManager holds definition of the Data Source and an interface to read, write, delete and update (CRUD)
    models withing the DataSource
    """
    __metaclass__ = ABCMeta

    def __init__(self, logger):
        super(BaseManager, self).__init__()
        self.logger = logger

    @abstractmethod
    def get(self, table_name, primary_key):
        pass

    @abstractmethod
    def filter(self, table_name, query):
        pass

    @abstractmethod
    def update(self, table_name, instance):
        pass

    @abstractmethod
    def delete(self, table_name, primary_key):
        pass

    @abstractmethod
    def highest_primary_key(self, table_name, timeperiod_low, timeperiod_high):
        pass

    @abstractmethod
    def lowest_primary_key(self, table_name, timeperiod_low, timeperiod_high):
        pass


class MongoDbManager(BaseManager):
    def __init__(self, logger):
        super(MongoDbManager, self).__init__(logger)
        self._db_client = MongoClient(settings['mongodb_host_list'])
        self._db = self._db_client[settings['mongo_db_name']]

    def __del__(self):
        try:
            self._db_client.close()
        except AttributeError:
            pass

    def connection(self, table_name):
        return self._db[table_name]

    def filter(self, table_name, query):
        conn = self._db[table_name]
        return conn.find(query)

    def delete(self, table_name, primary_key):
        conn = self._db[table_name]
        return conn.remove(primary_key, safe=True)

    def get(self, table_name, primary_key):
        query = {'_id': primary_key}
        conn = self._db[table_name]
        db_entry = conn.find_one(query)
        if db_entry is None:
            msg = 'Instance with ID=%s was not found' % str(primary_key)
            self.logger.warning(msg)
            raise LookupError(msg)
        return db_entry

    def insert(self, table_name, instance):
        conn = self._db[table_name]
        return conn.insert(instance, safe=True)

    def update(self, table_name, instance):
        conn = self._db[table_name]
        conn.save(instance, safe=True)

    def highest_primary_key(self, table_name, timeperiod_low, timeperiod_high):
        query = {base_model.TIMEPERIOD: {'$gte': timeperiod_low, '$lt': timeperiod_high}}
        conn = self._db[table_name]
        asc_search = conn.find(spec=query, fields='_id').sort('_id', ASCENDING).limit(1)
        if asc_search.count() == 0:
            raise LookupError('No messages in timeperiod: %s:%s in collection %s'
                              % (timeperiod_low, timeperiod_high, table_name))
        return asc_search[0]['_id']

    def lowest_primary_key(self, table_name, timeperiod_low, timeperiod_high):
        query = {base_model.TIMEPERIOD: {'$gte': timeperiod_low, '$lt': timeperiod_high}}
        conn = self._db[table_name]
        dec_search = conn.find(spec=query, fields='_id').sort('_id', DESCENDING).limit(1)
        last_object_id = dec_search[0]['_id']
        return last_object_id


class HBaseManager(BaseManager):
    pass