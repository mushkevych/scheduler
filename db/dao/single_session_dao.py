__author__ = 'Bohdan Mushkevych'

from threading import RLock
from db.manager import ds_manager
from db.model.raw_data import *
from db.model.single_session import SingleSession
from system.decorator import thread_safe
from system.collection_context import COLLECTION_SINGLE_SESSION


class SingleSessionDao(object):
    """ Thread-safe Data Access Object for single_session table/collection """
    def __init__(self, logger):
        super(SingleSessionDao, self).__init__()
        self.logger = logger
        self.lock = RLock()
        self.ds = ds_manager.ds_factory(logger)

    @thread_safe
    def get_one(self, domain_name, session_id):
        collection = self.ds.connection(COLLECTION_SINGLE_SESSION)

        query = {DOMAIN_NAME: domain_name,
                 FAMILY_USER_PROFILE + '.' + SESSION_ID: session_id}
        document = collection.find_one(query)
        if document is None:
            raise LookupError('MongoDB has no single session record for (%s, %s)' % (domain_name, session_id))
        return SingleSession(document)

    @thread_safe
    def insert(self, instance):
        """ method inserts new Single Session"""
        collection = self.ds.connection(COLLECTION_SINGLE_SESSION)
        return collection.insert(instance.document, safe=True)

    @thread_safe
    def update(self, instance, is_safe):
        """ method finds Single Session record and update its DB representation"""
        collection = self.ds.connection(COLLECTION_SINGLE_SESSION)
        return collection.save(instance.document, safe=is_safe)
