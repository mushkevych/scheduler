"""
Created on 2012-05-18

@author: Bohdan Mushkevych
"""
from model.scheduler_configuration_entry import SchedulerConfigurationEntry
from system.collection_context import COLLECTION_SCHEDULER_CONFIGURATION
from system.collection_context import CollectionContext


def retrieve(logger, process_name):
    """ method finds scheduler_configuration record and returns it to the caller"""
    query = { 'process_name' : process_name }
    collection = CollectionContext.get_collection(logger, COLLECTION_SCHEDULER_CONFIGURATION)
    db_entry = collection.find_one(query)
    if db_entry is None:
        msg = 'SchedulerConfigurationEntry for process=%s was not found' % str(process_name)
        logger.warning(msg)
        raise LookupError(msg)
    return SchedulerConfigurationEntry(db_entry)


def update(logger, scheduler_configuration):
    """ method finds scheduler_configuration record and update its DB representation"""
    w_number = CollectionContext.get_w_number(logger, COLLECTION_SCHEDULER_CONFIGURATION)
    collection = CollectionContext.get_collection(logger, COLLECTION_SCHEDULER_CONFIGURATION)
    collection.save(scheduler_configuration.document, safe=True, w=w_number)
