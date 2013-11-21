from abc import abstractmethod, ABCMeta

__author__ = 'Bohdan Mushkevych'

from pymongo import MongoClient
from settings import settings


def factory():
    # the only way to implement nonlocal closure variables in Python 2.X
    instances = {}

    def get_instance(logger):
        ds_type = settings['ds_type']

        if ds_type not in instances:
            if type == "mongo_db":
                instances[ds_type] = MongoDbManager(logger)
            elif type == "hbase":
                instances[ds_type] = HBaseManager(logger)
            else:
                raise ValueError('Unsupported Data Source type')
        return instances[ds_type]

    return get_instance


class BaseManager(object):
    """
    AbstractManager holds definition of the Data Source and an interface to read, write, delete and update (CRUD)
    models withing the DataSource
    """
    __metaclass__ = ABCMeta

    def __init__(self, logger):
        super(BaseManager, self).__init__()
        self.logger = logger

    @abstractmethod
    def get(self, table, primary_key):
        pass

    @abstractmethod
    def filter(self, table, query):
        pass

    @abstractmethod
    def update(self, table, instance):
        pass

    @abstractmethod
    def delete(self, table, primary_key):
        pass


class MongoDbManager(BaseManager):
    def __init__(self, logger):
        super(MongoDbManager, self).__init__(logger)
        self._db_client = MongoClient(settings['rs_system_host_list'])
        self._db = self._db_client[settings['mongo_db_name']]

    def __del__(self):
        try:
            self._db_client.close()
        except AttributeError:
            pass

    def filter(self, table, query):
        conn = self._db[table]
        return conn.find(query)

    def delete(self, table, primary_key):
        conn = self._db[table]
        return conn.remove(primary_key, safe=True)

    def get(self, table, primary_key):
        query = {'_id': primary_key}
        conn = self._db[table]
        db_entry = conn.find_one(query)
        if db_entry is None:
            msg = 'Instance with ID=%s was not found' % str(primary_key)
            self.logger.warning(msg)
            raise LookupError(msg)
        return db_entry

    def update(self, table, instance):
        conn = self._db[table]
        conn.save(instance, safe=True)


class HBaseManager(BaseManager):
    pass