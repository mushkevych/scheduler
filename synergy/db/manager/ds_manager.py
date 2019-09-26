__author__ = 'Bohdan Mushkevych'

import atexit
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson.objectid import ObjectId

from synergy.conf import settings
from synergy.db.model.unit_of_work import TIMEPERIOD
from odm.document import BaseDocument

QUERY_GET_ALL = {}

if 'ds_factory' not in globals():
    # this block defines module-level variable ds_factory

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
                    raise ValueError(f'Unsupported Data Source type: {ds_type}')
                atexit.register(instances[ds_type].interpreter_terminating)
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
        self.interpreter_is_terminating = False

    def __str__(self):
        raise NotImplementedError(f'method __str__ must be implemented by {self.__class__.__name__}')

    def interpreter_terminating(self):
        """ method is registered with the atexit hook, and notifies about Python interpreter shutdown sequnce """
        self.interpreter_is_terminating = True

    def is_alive(self):
        """ :return: True if the database server is available. False otherwise """
        raise NotImplementedError(f'method is_alive must be implemented by {self.__class__.__name__}')

    def get(self, table_name, primary_key):
        raise NotImplementedError(f'method get must be implemented by {self.__class__.__name__}')

    def filter(self, table_name, query):
        raise NotImplementedError(f'method filter must be implemented by {self.__class__.__name__}')

    def update(self, table_name, primary_key, instance):
        raise NotImplementedError(f'method update must be implemented by {self.__class__.__name__}')

    def delete(self, table_name, primary_key):
        raise NotImplementedError(f'method delete must be implemented by {self.__class__.__name__}')

    def highest_primary_key(self, table_name, timeperiod_low, timeperiod_high):
        raise NotImplementedError(f'method highest_primary_key must be implemented by {self.__class__.__name__}')

    def lowest_primary_key(self, table_name, timeperiod_low, timeperiod_high):
        raise NotImplementedError(f'method lowest_primary_key must be implemented by {self.__class__.__name__}')

    def cursor_fine(self,
                    table_name,
                    start_id_obj,
                    end_id_obj,
                    iteration,
                    start_timeperiod,
                    end_timeperiod):
        """ method returns DB cursor based on precise boundaries """
        raise NotImplementedError(f'method cursor_fine must be implemented by {self.__class__.__name__}')

    def cursor_batch(self,
                     table_name,
                     start_timeperiod,
                     end_timeperiod):
        """ method returns batched DB cursor """
        raise NotImplementedError(f'method cursor_batch must be implemented by {self.__class__.__name__}')


class MongoDbManager(BaseManager):
    def __init__(self, logger):
        super(MongoDbManager, self).__init__(logger)
        self._db_client = MongoClient(settings.settings['mongodb_host_list'])
        self._db = self._db_client[settings.settings['mongo_db_name']]

    def __del__(self):
        try:
            self._db_client.close()
        except Exception as e:
            if self.interpreter_is_terminating:
                self.logger.error(f'MongoDbManager cleanup likely followed MongoClient cleanup: {e}')
            else:
                self.logger.error(f'Exception on closing MongoClient: {e}', exc_info=True)
        finally:
            self._db = None
            self._db_client = None

    def __str__(self):
        return f'MongoDbManager: {settings.settings["mongodb_host_list"]}@{settings.settings["mongo_db_name"]}'

    def is_alive(self):
        return self._db_client.admin.command('ping')

    def connection(self, table_name):
        return self._db[table_name]

    def filter(self, table_name, query):
        conn = self._db[table_name]
        return conn.find(query)

    def delete(self, table_name, primary_key: dict):
        conn = self._db[table_name]
        conn.delete_one(primary_key)

    def get(self, table_name, primary_key: dict):
        conn = self._db[table_name]
        db_entry = conn.find_one(primary_key)
        if db_entry is None:
            msg = f'Instance with ID={primary_key} was not found'
            self.logger.warning(msg)
            raise LookupError(msg)
        return db_entry

    def insert(self, table_name, instance: BaseDocument):
        conn = self._db[table_name]
        return conn.insert_one(instance.document).inserted_id

    def update(self, table_name, primary_key: dict, instance: BaseDocument):
        """ replaces document identified by the primary_key or creates one if a matching document does not exist"""
        collection = self._db[table_name]

        # work with a copy of the document, as the direct type change of the _id field
        # is later negated by the `BaseDocument.to_json` method
        document = instance.document
        if '_id' in document:
            document['_id'] = ObjectId(document['_id'])

        update_result = collection.replace_one(filter=primary_key, replacement=document, upsert=True)
        if update_result.upserted_id:
            instance['_id'] = update_result.upserted_id
        return update_result.upserted_id

    def highest_primary_key(self, table_name, timeperiod_low, timeperiod_high):
        query = {TIMEPERIOD: {'$gte': timeperiod_low, '$lt': timeperiod_high}}
        conn = self._db[table_name]
        asc_search = conn.find(filter=query, projection='_id').sort('_id', ASCENDING).limit(1)
        if asc_search.count() == 0:
            raise LookupError(
                f'No records in timeperiod: [{timeperiod_low} : {timeperiod_high}) in collection {table_name}')
        return asc_search[0]['_id']

    def lowest_primary_key(self, table_name, timeperiod_low, timeperiod_high):
        query = {TIMEPERIOD: {'$gte': timeperiod_low, '$lt': timeperiod_high}}
        conn = self._db[table_name]
        dec_search = conn.find(filter=query, projection='_id').sort('_id', DESCENDING).limit(1)
        if dec_search.count() == 0:
            raise LookupError(
                f'No records in timeperiod: [{timeperiod_low} : {timeperiod_high}) in collection {table_name}')
        return dec_search[0]['_id']

    def cursor_fine(self,
                    table_name,
                    start_id_obj,
                    end_id_obj,
                    iteration,
                    start_timeperiod,
                    end_timeperiod):
        if iteration == 0:
            queue = {'_id': {'$gte': ObjectId(start_id_obj), '$lte': ObjectId(end_id_obj)}}
        else:
            queue = {'_id': {'$gt': ObjectId(start_id_obj), '$lte': ObjectId(end_id_obj)}}

        if start_timeperiod is not None and end_timeperiod is not None:
            # remove all accident objects that may be in [start_id_obj : end_id_obj] range
            queue[TIMEPERIOD] = {'$gte': start_timeperiod, '$lt': end_timeperiod}

        conn = self._db[table_name]
        batch_size = settings.settings['batch_size']
        return conn.find(queue).sort('_id', ASCENDING).limit(batch_size)

    def cursor_batch(self, table_name, start_timeperiod, end_timeperiod):
        assert start_timeperiod is not None and end_timeperiod is not None

        conn = self._db[table_name]
        batch_size = settings.settings['batch_size']

        queue = {TIMEPERIOD: {'$gte': start_timeperiod, '$lt': end_timeperiod}}
        return conn.find(queue).batch_size(batch_size)


class HBaseManager(BaseManager):
    pass
