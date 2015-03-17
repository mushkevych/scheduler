__author__ = 'Bohdan Mushkevych'

from pymongo import MongoClient, ASCENDING, DESCENDING
from bson.objectid import ObjectId

from synergy.conf import settings
from synergy.db.model.unit_of_work import TIMEPERIOD

QUERY_GET_ALL = {}

if 'ds_factory' not in globals():
    # this block defines global variable ds_factory

    def factory():
        # the only way to implement nonlocal closure variables in Python 2.X
        instances = {}

        def get_instance(logger):
            ds_type = settings.settings['ds_type']

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


class BaseManager(object):
    """
    BaseManager holds definition of the Data Source and an interface to read, write, delete and update (CRUD)
    models withing the DataSource
    """
    def __init__(self, logger):
        super(BaseManager, self).__init__()
        self.logger = logger

    def __str__(self):
        raise NotImplementedError('method __str__ must be implemented by {0}'.format(self.__class__.__name__))

    def is_alive(self):
        """ :return: True if the database server is available. False otherwise """
        raise NotImplementedError('method is_alive must be implemented by {0}'.format(self.__class__.__name__))

    def get(self, table_name, primary_key):
        raise NotImplementedError('method get must be implemented by {0}'.format(self.__class__.__name__))

    def filter(self, table_name, query):
        raise NotImplementedError('method filter must be implemented by {0}'.format(self.__class__.__name__))

    def update(self, table_name, instance):
        raise NotImplementedError('method update must be implemented by {0}'.format(self.__class__.__name__))

    def delete(self, table_name, primary_key):
        raise NotImplementedError('method delete must be implemented by {0}'.format(self.__class__.__name__))

    def highest_primary_key(self, table_name, timeperiod_low, timeperiod_high):
        raise NotImplementedError('method highest_primary_key must be implemented by {0}'.format(self.__class__.__name__))

    def lowest_primary_key(self, table_name, timeperiod_low, timeperiod_high):
        raise NotImplementedError('method lowest_primary_key must be implemented by {0}'.format(self.__class__.__name__))

    def cursor_for(self,
                   table_name,
                   start_id_obj,
                   end_id_obj,
                   iteration,
                   start_timeperiod,
                   end_timeperiod,
                   bulk_threshold):
        raise NotImplementedError('method cursor_for must be implemented by {0}'.format(self.__class__.__name__))


class MongoDbManager(BaseManager):
    def __init__(self, logger):
        super(MongoDbManager, self).__init__(logger)
        self._db_client = MongoClient(settings.settings['mongodb_host_list'])
        self._db = self._db_client[settings.settings['mongo_db_name']]

    def __del__(self):
        try:
            self._db_client.close()
        except AttributeError:
            pass

    def __str__(self):
        return 'MongoDbManager: %s@%s' % (settings.settings['mongodb_host_list'], settings.settings['mongo_db_name'])

    def is_alive(self):
        return self._db_client.alive()

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
            self.logger.warn(msg)
            raise LookupError(msg)
        return db_entry

    def insert(self, table_name, instance):
        conn = self._db[table_name]
        return conn.insert(instance, safe=True)

    def update(self, table_name, instance):
        conn = self._db[table_name]
        conn.save(instance, safe=True)

    def highest_primary_key(self, table_name, timeperiod_low, timeperiod_high):
        query = {TIMEPERIOD: {'$gte': timeperiod_low, '$lt': timeperiod_high}}
        conn = self._db[table_name]
        asc_search = conn.find(spec=query, fields='_id').sort('_id', ASCENDING).limit(1)
        if asc_search.count() == 0:
            raise LookupError('No messages in timeperiod: %s:%s in collection %s'
                              % (timeperiod_low, timeperiod_high, table_name))
        return asc_search[0]['_id']

    def lowest_primary_key(self, table_name, timeperiod_low, timeperiod_high):
        query = {TIMEPERIOD: {'$gte': timeperiod_low, '$lt': timeperiod_high}}
        conn = self._db[table_name]
        dec_search = conn.find(spec=query, fields='_id').sort('_id', DESCENDING).limit(1)
        last_object_id = dec_search[0]['_id']
        return last_object_id

    def cursor_for(self,
                   table_name,
                   start_id_obj,
                   end_id_obj,
                   iteration,
                   start_timeperiod,
                   end_timeperiod,
                   bulk_threshold):
        if not isinstance(start_id_obj, ObjectId):
            start_id_obj = ObjectId(start_id_obj)
        if not isinstance(end_id_obj, ObjectId):
            end_id_obj = ObjectId(end_id_obj)

        if iteration == 0:
            queue = {'_id': {'$gte': start_id_obj, '$lte': end_id_obj}}
        else:
            queue = {'_id': {'$gt': start_id_obj, '$lte': end_id_obj}}

        if start_timeperiod is not None and end_timeperiod is not None:
            # remove all accident objects that may be in [start_id_obj : end_id_obj] range
            queue[TIMEPERIOD] = {'$gte': start_timeperiod, '$lt': end_timeperiod}

        conn = self._db[table_name]
        return conn.find(queue).sort('_id', ASCENDING).limit(bulk_threshold)


class HBaseManager(BaseManager):
    pass
