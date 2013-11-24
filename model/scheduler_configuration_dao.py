__author__ = 'Bohdan Mushkevych'

from model import ds_manager
from model.scheduler_configuration import SchedulerConfiguration
from system.collection_context import COLLECTION_SCHEDULER_CONFIGURATION


def get_one(logger, key):
    """ method finds scheduler_configuration record and returns it to the caller"""
    query = {'process_name': key}
    ds = ds_manager.ds_factory(logger)
    collection = ds.connection(COLLECTION_SCHEDULER_CONFIGURATION)

    document = collection.find_one(query)
    if document is None:
        raise LookupError('SchedulerConfiguration for process=%s was not found' % str(key))
    return SchedulerConfiguration(document)


def get_all(logger):
    query = {}
    ds = ds_manager.ds_factory(logger)
    collection = ds.connection(COLLECTION_SCHEDULER_CONFIGURATION)

    cursor = collection.find(query)
    if cursor.count() == 0:
        raise LookupError('MongoDB has no scheduler configuration entries')
    return [SchedulerConfiguration(entry) for entry in cursor]


def update(logger, instance):
    """ method finds scheduler_configuration record and update its DB representation"""
    ds = ds_manager.ds_factory(logger)
    collection = ds.connection(COLLECTION_SCHEDULER_CONFIGURATION)
    collection.save(instance.document, safe=True)
