__author__ = 'Bohdan Mushkevych'

from bson import ObjectId
from threading import RLock

from synergy.db.manager import ds_manager
from synergy.db.model.freerun_process_entry import FreerunProcessEntry
from synergy.system.decorator import thread_safe
from synergy.scheduler.scheduler_constants import COLLECTION_FREERUN_PROCESS


class FreerunProcessDao(object):
    """ Thread-safe Data Access Object for freerun_process table/collection """

    def __init__(self, logger):
        super(FreerunProcessDao, self).__init__()
        self.logger = logger
        self.lock = RLock()
        self.ds = ds_manager.ds_factory(logger)

    @thread_safe
    def get_one(self, key):
        """ method finds freerun_process record and returns it to the caller"""
        assert not isinstance(key, str)
        query = {'process_name': key[0], 'entry_name': key[1]}
        collection = self.ds.connection(COLLECTION_FREERUN_PROCESS)

        document = collection.find_one(query)
        if document is None:
            raise LookupError('FreerunProcessEntry for process=%s was not found' % str(key))
        return FreerunProcessEntry.from_json(document)

    @thread_safe
    def get_all(self):
        query = {}
        collection = self.ds.connection(COLLECTION_FREERUN_PROCESS)

        cursor = collection.find(query)
        if cursor.count() == 0:
            raise LookupError('MongoDB has no FreerunProcessEntry records')
        return [FreerunProcessEntry.from_json(entry) for entry in cursor]

    @thread_safe
    def update(self, instance):
        """ method finds freerun_process record and update its DB representation"""
        assert isinstance(instance, FreerunProcessEntry)
        collection = self.ds.connection(COLLECTION_FREERUN_PROCESS)
        document = instance.document
        if instance.db_id:
            document['_id'] = ObjectId(instance.db_id)
        instance.db_id = collection.save(document, safe=True)
        return instance.db_id

    @thread_safe
    def remove(self, key):
        assert not isinstance(key, str)
        query = {'process_name': key[0], 'entry_name': key[1]}
        collection = self.ds.connection(COLLECTION_FREERUN_PROCESS)
        return collection.remove(query, safe=True)
