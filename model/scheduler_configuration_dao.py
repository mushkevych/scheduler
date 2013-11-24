__author__ = 'Bohdan Mushkevych'

from model.scheduler_configuration import SchedulerConfiguration
from system.collection_context import COLLECTION_SCHEDULER_CONFIGURATION
from system.collection_context import CollectionContext


def get_one(logger, key):
    """ method finds scheduler_configuration record and returns it to the caller"""
    query = {'process_name': key}
    collection = CollectionContext.get_collection(logger, COLLECTION_SCHEDULER_CONFIGURATION)
    document = collection.find_one(query)
    if document is None:
        raise LookupError('SchedulerConfiguration for process=%s was not found' % str(key))
    return SchedulerConfiguration(document)


def get_all(logger):
    query = {}
    collection = CollectionContext.get_collection(logger, COLLECTION_SCHEDULER_CONFIGURATION)
    cursor = collection.find(query)
    if cursor.count() == 0:
        raise LookupError('MongoDB has no scheduler configuration entries')
    return [SchedulerConfiguration(entry) for entry in cursor]


def update(logger, instance):
    """ method finds scheduler_configuration record and update its DB representation"""
    collection = CollectionContext.get_collection(logger, COLLECTION_SCHEDULER_CONFIGURATION)
    collection.save(instance.document, safe=True)
