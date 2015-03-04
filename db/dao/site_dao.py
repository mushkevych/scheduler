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

        query = {DOMAIN_NAME: domain_name,
                 TIMEPERIOD: timeperiod}
        document = collection.find_one(query)
        if document is None:
            raise LookupError('MongoDB has no site record in %s for (%s, %s)'
                              % (collection_name, domain_name, timeperiod))
        return SiteStatistics.from_json(document)

    @thread_safe
    def update(self, collection_name, instance, is_safe):
        """ method finds Site Statistics record and update it DB representation """
        assert isinstance(instance, SiteStatistics)
        collection = self.ds.connection(collection_name)
        document = instance.document
        if instance.db_id:
            document['_id'] = ObjectId(instance.db_id)
        instance.db_id = collection.save(document, safe=is_safe)
        return instance.db_id
