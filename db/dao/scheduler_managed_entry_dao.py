__author__ = 'Bohdan Mushkevych'

from threading import RLock
from db.manager import ds_manager
from db.model.scheduler_managed_entry import SchedulerManagedEntry
from system.decorator import thread_safe
from system.collection_context import COLLECTION_SCHEDULER_MANAGED_ENTRIES


class SchedulerManagedEntryDao(object):
    """ Thread-safe Data Access Object for scheduler_managed_entry table/collection """

    def __init__(self, logger):
        super(SchedulerManagedEntryDao, self).__init__()
        self.logger = logger
        self.lock = RLock()
        self.ds = ds_manager.ds_factory(logger)

    @thread_safe
    def get_one(self, key):
        """ method finds scheduler_managed_entry record and returns it to the caller"""
        query = {'process_name': key}
        collection = self.ds.connection(COLLECTION_SCHEDULER_MANAGED_ENTRIES)

        document = collection.find_one(query)
        if document is None:
            raise LookupError('SchedulerManagedEntry for process=%s was not found' % str(key))
        return SchedulerManagedEntry(document)

    @thread_safe
    def get_all(self):
        query = {}
        collection = self.ds.connection(COLLECTION_SCHEDULER_MANAGED_ENTRIES)

        cursor = collection.find(query)
        if cursor.count() == 0:
            raise LookupError('MongoDB has no SchedulerManagedEntry records')
        return [SchedulerManagedEntry(entry) for entry in cursor]

    @thread_safe
    def update(self, instance):
        """ method finds scheduler_managed_entry record and update its DB representation"""
        assert isinstance(instance, SchedulerManagedEntry)
        collection = self.ds.connection(COLLECTION_SCHEDULER_MANAGED_ENTRIES)
        return collection.save(instance.document, safe=True)
