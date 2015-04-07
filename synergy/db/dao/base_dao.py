__author__ = 'Bohdan Mushkevych'

from bson import ObjectId
from threading import RLock

from synergy.db.manager import ds_manager
from synergy.system.decorator import thread_safe


class BaseDao(object):
    """ Thread-safe base Data Access Object """

    def __init__(self, logger, model_class, primary_key, collection_name):
        super(BaseDao, self).__init__()
        self.logger = logger
        self.model_klass = model_class
        self.primary_key = primary_key
        self.collection_name = collection_name

        self.lock = RLock()
        self.ds = ds_manager.ds_factory(logger)

    def _tuple_to_query(self, key_tuple):
        if isinstance(key_tuple, str):
            key_tuple = [key_tuple]
        assert len(key_tuple) == len(self.primary_key)

        query = dict()
        for k, v in zip(self.primary_key, key_tuple):
            query[k] = v
        return query

    @thread_safe
    def get_one(self, key):
        """ method finds single record base on the given primary key and returns it to the caller"""
        query = self._tuple_to_query(key)
        collection = self.ds.connection(self.collection_name)

        document = collection.find_one(query)
        if document is None:
            raise LookupError('%s with key %r was not found' % (self.model_klass.__name__, query))
        return self.model_klass.from_json(document)

    @thread_safe
    def run_query(self, query):
        """ method runs query on a specified collection and return a list of filtered Model records """
        collection = self.ds.connection(self.collection_name)

        cursor = collection.find(query)
        if cursor.count() == 0:
            raise LookupError('Collection %s has no %s records' %
                              (self.collection_name, self.model_klass.__name__))
        return [self.model_klass.from_json(entry) for entry in cursor]

    @thread_safe
    def get_all(self):
        return self.run_query({})

    @thread_safe
    def update(self, instance):
        """ this is upsert method: inserts or updates the DB representation of the model instance """
        assert isinstance(instance, self.model_klass)
        collection = self.ds.connection(self.collection_name)
        document = instance.document
        if instance.db_id:
            document['_id'] = ObjectId(instance.db_id)
        instance.db_id = collection.save(document, safe=True)
        return instance.db_id

    @thread_safe
    def remove(self, key):
        query = self._tuple_to_query(key)
        collection = self.ds.connection(self.collection_name)
        return collection.remove(query, safe=True)
