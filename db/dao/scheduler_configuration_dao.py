__author__ = 'Bohdan Mushkevych'

from threading import RLock
from db.manager import ds_manager
from db.model.scheduler_configuration import SchedulerConfiguration
from system.decorator import thread_safe
from system.collection_context import COLLECTION_SCHEDULER_CONFIGURATION


class SchedulerConfigurationDao(object):
    """ Thread-safe Data Access Object for scheduler_configuration table/collection """

    def __init__(self, logger):
        super(SchedulerConfigurationDao, self).__init__()
        self.logger = logger
        self.lock = RLock()
        self.ds = ds_manager.ds_factory(logger)

    @thread_safe
    def get_one(self, key):
        """ method finds scheduler_configuration record and returns it to the caller"""
        query = {'process_name': key}
        collection = self.ds.connection(COLLECTION_SCHEDULER_CONFIGURATION)

        document = collection.find_one(query)
        if document is None:
            raise LookupError('SchedulerConfiguration for process=%s was not found' % str(key))
        return SchedulerConfiguration(document)

    @thread_safe
    def get_all(self):
        query = {}
        collection = self.ds.connection(COLLECTION_SCHEDULER_CONFIGURATION)

        cursor = collection.find(query)
        if cursor.count() == 0:
            raise LookupError('MongoDB has no scheduler configuration entries')
        return [SchedulerConfiguration(entry) for entry in cursor]

    @thread_safe
    def update(self, instance):
        """ method finds scheduler_configuration record and update its DB representation"""
        collection = self.ds.connection(COLLECTION_SCHEDULER_CONFIGURATION)
        return collection.save(instance.document, safe=True)
