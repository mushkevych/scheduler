"""
Created on 2011-04-67

@author: Bohdan Mushkevych
"""
from model.unit_of_work_entry import UnitOfWorkEntry
from system.collection_context import COLLECTION_UNITS_OF_WORK
from system.collection_context import CollectionContext

def retrieve_by_id(logger, object_id):
    """ method finds unit_of_work record and returns it to the caller"""
    query = { '_id' : object_id }
    collection = CollectionContext.get_collection(logger, COLLECTION_UNITS_OF_WORK)
    db_entry = collection.find_one(query)
    if db_entry is None:
        msg = 'Unit_of_work with ID=%s was not found' % str(object_id)
        logger.warning(msg)
        raise LookupError(msg)
    return UnitOfWorkEntry(db_entry)

def retrieve_by_params(logger, process_name, timestamp, start_obj_id, end_obj_id):
    """ method finds unit_of_work record and returns it to the caller"""
    query = { UnitOfWorkEntry.PROCESS_NAME : process_name,
              UnitOfWorkEntry.TIMESTAMP : timestamp,
              UnitOfWorkEntry.START_OBJ_ID : start_obj_id,
              UnitOfWorkEntry.END_OBJ_ID : end_obj_id}
    collection = CollectionContext.get_collection(logger, COLLECTION_UNITS_OF_WORK)
    db_entry = collection.find_one(query)
    if db_entry is None:
        msg = 'Unit_of_work satisfying query %r was not found' % query
        logger.warning(msg)
        raise LookupError(msg)
    return UnitOfWorkEntry(db_entry)

def update(logger, unit_of_work):
    """ method finds unit_of_work record and change its status"""
    w_number = CollectionContext.get_w_number(logger, COLLECTION_UNITS_OF_WORK)
    collection = CollectionContext.get_collection(logger, COLLECTION_UNITS_OF_WORK)
    collection.save(unit_of_work.get_document(), safe=True, w=w_number)

def insert(logger, unit_of_work):
    """ inserts unit of work to MongoDB. @throws DuplicateKeyError is such record already exists """
    w_number = CollectionContext.get_w_number(logger, COLLECTION_UNITS_OF_WORK)
    collection = CollectionContext.get_collection(logger, COLLECTION_UNITS_OF_WORK)
    uow_id = collection.insert(unit_of_work.get_document(), safe=True, w=w_number)
    return uow_id

def remove(logger, uow_id):
    w_number = CollectionContext.get_w_number(logger, COLLECTION_UNITS_OF_WORK)
    collection = CollectionContext.get_collection(logger, COLLECTION_UNITS_OF_WORK)
    collection.remove(uow_id, safe=True, w=w_number)