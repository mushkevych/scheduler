__author__ = 'Bohdan Mushkevych'

from model import unit_of_work
from model.unit_of_work import UnitOfWork
from bson.objectid import ObjectId
from system.collection_context import COLLECTION_UNITS_OF_WORK
from system.collection_context import CollectionContext


def get_one(logger, key):
    """ method finds unit_of_work record and returns it to the caller"""
    if not isinstance(key, ObjectId):
        # cast key to ObjectId
        key = ObjectId(key)

    query = {'_id': key}
    collection = CollectionContext.get_collection(logger, COLLECTION_UNITS_OF_WORK)
    db_entry = collection.find_one(query)
    if db_entry is None:
        msg = 'Unit_of_work with ID=%s was not found' % str(key)
        logger.warning(msg)
        raise LookupError(msg)
    return UnitOfWork(db_entry)


def get_by_params(logger, process_name, timeperiod, start_obj_id, end_obj_id):
    """ method finds unit_of_work record and returns it to the caller"""
    query = {unit_of_work.PROCESS_NAME: process_name,
             unit_of_work.TIMEPERIOD: timeperiod,
             unit_of_work.START_OBJ_ID: start_obj_id,
             unit_of_work.END_OBJ_ID: end_obj_id}
    collection = CollectionContext.get_collection(logger, COLLECTION_UNITS_OF_WORK)
    db_entry = collection.find_one(query)
    if db_entry is None:
        raise LookupError('Unit_of_work satisfying query %r was not found' % query)
    return UnitOfWork(db_entry)


def update(logger, unit_of_work):
    """ method finds unit_of_work record and change its status"""
    collection = CollectionContext.get_collection(logger, COLLECTION_UNITS_OF_WORK)
    collection.save(unit_of_work.document, safe=True)


def insert(logger, unit_of_work):
    """ inserts unit of work to MongoDB. @throws DuplicateKeyError if such record already exist """
    collection = CollectionContext.get_collection(logger, COLLECTION_UNITS_OF_WORK)
    uow_id = collection.insert(unit_of_work.document, safe=True)
    return uow_id


def remove(logger, uow_id):
    collection = CollectionContext.get_collection(logger, COLLECTION_UNITS_OF_WORK)
    collection.remove(uow_id, safe=True)