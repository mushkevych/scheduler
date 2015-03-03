__author__ = 'Bohdan Mushkevych'

from bson import ObjectId
from threading import RLock
from synergy.db.manager import ds_manager
from synergy.db.model.managed_process_entry import ManagedProcessEntry
from synergy.system.decorator import thread_safe
from synergy.scheduler.scheduler_constants import COLLECTION_MANAGED_PROCESS


class ManagedProcessDao(object):
    """ Thread-safe Data Access Object for managed_process table/collection """

    def __init__(self, logger):
        super(ManagedProcessDao, self).__init__()
        self.logger = logger
        self.lock = RLock()
        self.ds = ds_manager.ds_factory(logger)

    @thread_safe
    def get_one(self, key):
        """ method finds managed_process record and returns it to the caller"""
        query = {'process_name': key}
        collection = self.ds.connection(COLLECTION_MANAGED_PROCESS)

        document = collection.find_one(query)
        if document is None:
            raise LookupError('ManagedProcessEntry for process=%s was not found' % str(key))
        return ManagedProcessEntry.from_json(document)

    @thread_safe
    def get_all(self):
        query = {}
        collection = self.ds.connection(COLLECTION_MANAGED_PROCESS)

        cursor = collection.find(query)
        if cursor.count() == 0:
            raise LookupError('MongoDB has no ManagedProcessEntry records')
        return [ManagedProcessEntry.from_json(entry) for entry in cursor]

    @thread_safe
    def update(self, instance):
        """ method finds managed_process record and update its DB representation"""
        assert isinstance(instance, ManagedProcessEntry)
        collection = self.ds.connection(COLLECTION_MANAGED_PROCESS)
        document = instance.document
        if instance.db_id:
            document['_id'] = ObjectId(instance.db_id)
        instance.db_id = collection.save(document, safe=True)
        return instance.db_id

    @thread_safe
    def remove(self, key):
        assert isinstance(key, str)
        query = {'process_name': key}
        collection = self.ds.connection(COLLECTION_MANAGED_PROCESS)
        return collection.remove(query, safe=True)

    @thread_safe
    def clear(self):
        """ removes all documents in this collection """
        collection = self.ds.connection(COLLECTION_MANAGED_PROCESS)
        return collection.remove()
