from db.manager import ds_manager
from db.model import unit_of_work

__author__ = 'Bohdan Mushkevych'

from db.model.unit_of_work import UnitOfWork
from bson.objectid import ObjectId
from system.collection_context import COLLECTION_UNITS_OF_WORK


def get_one(logger, key):
    """ method finds unit_of_work record and returns it to the caller"""
    if not isinstance(key, ObjectId):
        # cast key to ObjectId
        key = ObjectId(key)

    query = {'_id': key}
    ds = ds_manager.ds_factory(logger)
    collection = ds.connection(COLLECTION_UNITS_OF_WORK)

    document = collection.find_one(query)
    if document is None:
        msg = 'Unit_of_work with ID=%s was not found' % str(key)
        logger.warning(msg)
        raise LookupError(msg)
    return UnitOfWork(document)


def get_by_params(logger, process_name, timeperiod, start_obj_id, end_obj_id):
    """ method finds unit_of_work record and returns it to the caller"""
    query = {unit_of_work.PROCESS_NAME: process_name,
             unit_of_work.TIMEPERIOD: timeperiod,
             unit_of_work.START_OBJ_ID: start_obj_id,
             unit_of_work.END_OBJ_ID: end_obj_id}
    ds = ds_manager.ds_factory(logger)
    collection = ds.connection(COLLECTION_UNITS_OF_WORK)

    document = collection.find_one(query)
    if document is None:
        raise LookupError('Unit_of_work satisfying query %r was not found' % query)
    return UnitOfWork(document)


def update(logger, unit_of_work):
    """ method finds unit_of_work record and change its status"""
    ds = ds_manager.ds_factory(logger)
    collection = ds.connection(COLLECTION_UNITS_OF_WORK)
    collection.save(unit_of_work.document, safe=True)


def insert(logger, unit_of_work):
    """ inserts unit of work to MongoDB. @throws DuplicateKeyError if such record already exist """
    ds = ds_manager.ds_factory(logger)
    collection = ds.connection(COLLECTION_UNITS_OF_WORK)
    return collection.insert(unit_of_work.document, safe=True)


def remove(logger, uow_id):
    ds = ds_manager.ds_factory(logger)
    collection = ds.connection(COLLECTION_UNITS_OF_WORK)
    collection.remove(uow_id, safe=True)