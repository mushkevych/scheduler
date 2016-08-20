__author__ = 'Bohdan Mushkevych'

from threading import RLock

from bson import ObjectId
from six import string_types

from synergy.db.manager import ds_manager
from synergy.system.decorator import thread_safe


def build_db_query(fields_names, field_values):
    """ method builds query dictionary by zipping together DB field names with the field values """
    if isinstance(field_values, string_types):
        field_values = [field_values]

    if len(fields_names) != len(field_values):
        raise ValueError('Error: unable to build a primary key query due '
                         'to mismatch in number of fields {0} vs {1}'
                         .format(len(fields_names), len(field_values)))

    query = dict()
    for k, v in zip(fields_names, field_values):
        query[k] = v
    return query


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

    @thread_safe
    def get_one(self, key):
        """ method finds single record base on the given primary key and returns it to the caller"""
        query = build_db_query(self.primary_key, key)
        collection = self.ds.connection(self.collection_name)

        document = collection.find_one(query)
        if document is None:
            raise LookupError('{0} with key {1} was not found'.format(self.model_klass.__name__, query))
        return self.model_klass.from_json(document)

    @thread_safe
    def run_query(self, query):
        """ method runs query on a specified collection and return a list of filtered Model records """
        collection = self.ds.connection(self.collection_name)

        cursor = collection.find(query)
        if cursor.count() == 0:
            raise LookupError('Collection {0} has no {1} records'
                              .format(self.collection_name, self.model_klass.__name__))
        return [self.model_klass.from_json(entry) for entry in cursor]

    @thread_safe
    def get_all(self):
        return self.run_query({})

    @thread_safe
    def update(self, instance):
        """ this is an upsert method: replaces or creates the DB representation of the model instance """
        assert isinstance(instance, self.model_klass)
        if instance.db_id:
            query = {'_id': ObjectId(instance.db_id)}
        else:
            query = build_db_query(self.primary_key, instance.key)
        self.ds.update(self.collection_name, query, instance)
        return instance.db_id

    @thread_safe
    def remove(self, key):
        query = build_db_query(self.primary_key, key)
        collection = self.ds.connection(self.collection_name)
        collection.delete_one(query)
