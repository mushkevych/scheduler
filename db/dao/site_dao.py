__author__ = 'Bohdan Mushkevych'

from bson import ObjectId
from threading import RLock

from db.model.raw_data import *
from db.model.site_statistics import SiteStatistics
from synergy.db.manager import ds_manager
from synergy.system.decorator import thread_safe


class SiteDao(object):
    """ Thread-safe Data Access Object for site_XXX table/collection """
    def __init__(self, logger):
        super(SiteDao, self).__init__()
        self.logger = logger
        self.lock = RLock()
        self.ds = ds_manager.ds_factory(logger)

    @thread_safe
    def get_one(self, collection_name, domain_name, timeperiod):
        collection = self.ds.connection(collection_name)

        query = {DOMAIN_NAME: domain_name, TIMEPERIOD: timeperiod}
        document = collection.find_one(query)
        if document is None:
            raise LookupError('MongoDB has no site record in {0} for ({1}, {2})'
                              .format(collection_name, domain_name, timeperiod))
        return SiteStatistics.from_json(document)

    @thread_safe
    def update(self, collection_name, instance):
        """ method finds Site Statistics record and update it DB representation """
        assert isinstance(instance, SiteStatistics)
        collection = self.ds.connection(collection_name)
        document = instance.document
        if instance.db_id:
            document['_id'] = ObjectId(instance.db_id)
        instance.db_id = collection.save(document, safe=True)
        return instance.db_id

    @thread_safe
    def insert(self, collection_name, instance):
        """ inserts a unit of work into MongoDB. """
        assert isinstance(instance, SiteStatistics)
        collection = self.ds.connection(collection_name)
        return collection.insert_one(instance.document).inserted_id

    @thread_safe
    def remove(self, collection_name, domain_name, timeperiod):
        query = {DOMAIN_NAME: domain_name, TIMEPERIOD: timeperiod}
        collection = self.ds.connection(collection_name)
        collection.delete_one(query)
