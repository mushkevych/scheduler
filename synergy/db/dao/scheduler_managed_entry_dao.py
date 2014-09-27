__author__ = 'Bohdan Mushkevych'

from threading import RLock
from synergy.db.manager import ds_manager
from synergy.db.model.scheduler_managed_entry import SchedulerManagedEntry
from synergy.system.decorator import thread_safe
from synergy.scheduler.scheduler_constants import COLLECTION_SCHEDULER_MANAGED_ENTRY


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
        collection = self.ds.connection(COLLECTION_SCHEDULER_MANAGED_ENTRY)

        document = collection.find_one(query)
        if document is None:
            raise LookupError('SchedulerManagedEntry for process=%s was not found' % str(key))
        return SchedulerManagedEntry(document)

    @thread_safe
    def get_all(self):
        query = {}
        collection = self.ds.connection(COLLECTION_SCHEDULER_MANAGED_ENTRY)

        cursor = collection.find(query)
        if cursor.count() == 0:
            raise LookupError('MongoDB has no SchedulerManagedEntry records')
        return [SchedulerManagedEntry(entry) for entry in cursor]

    @thread_safe
    def update(self, instance):
        """ method finds scheduler_managed_entry record and update its DB representation"""
        assert isinstance(instance, SchedulerManagedEntry)
        collection = self.ds.connection(COLLECTION_SCHEDULER_MANAGED_ENTRY)
        return collection.save(instance.document, safe=True)

    @thread_safe
    def remove(self, key):
        assert isinstance(key, str)
        query = {'process_name': key}
        collection = self.ds.connection(COLLECTION_SCHEDULER_MANAGED_ENTRY)
        return collection.remove(query, safe=True)
