__author__ = 'Bohdan Mushkevych'

from model import time_table_record, base_model, ds_manager
from model.time_table_record import TimeTableRecord
from system.collection_context import COLLECTION_TIMETABLE_HOURLY, COLLECTION_TIMETABLE_DAILY, \
    COLLECTION_TIMETABLE_MONTHLY, COLLECTION_TIMETABLE_YEARLY
from system.process_context import ProcessContext


QUERY_GET_LIKE_TIMEPERIOD = \
    lambda timeperiod: {base_model.TIMEPERIOD: {'$regex': timeperiod}}

QUERY_GET_LIKE_TIMEPERIOD_AND_NOT_PROCESSED = \
    lambda timeperiod: {base_model.TIMEPERIOD: {'$regex': timeperiod},
                        time_table_record.STATE: {'$ne': time_table_record.STATE_PROCESSED}}


#@thread_safe
def _get_timetable_collection(logger, process_name):
    """timetable stores timeperiod in 4 collections: hourly, daily, monthly and yearly;
    method looks for the proper timetable_collection base on process TIME_QUALIFIER"""
    ds = ds_manager.ds_factory(logger)
    qualifier = ProcessContext.get_time_qualifier(process_name)

    if qualifier == ProcessContext.QUALIFIER_HOURLY:
        collection = ds.connection(COLLECTION_TIMETABLE_HOURLY)
    elif qualifier == ProcessContext.QUALIFIER_DAILY:
        collection = ds.connection(COLLECTION_TIMETABLE_DAILY)
    elif qualifier == ProcessContext.QUALIFIER_MONTHLY:
        collection = ds.connection(COLLECTION_TIMETABLE_MONTHLY)
    elif qualifier == ProcessContext.QUALIFIER_YEARLY:
        collection = ds.connection(COLLECTION_TIMETABLE_YEARLY)
    else:
        raise ValueError('Unknown time qualifier: %s for %s' % (qualifier, process_name))
    return collection


#@thread_safe
def get_one(logger, key, timeperiod):
    """ method finds time_table record and returns it to the caller"""
    collection = _get_timetable_collection(logger, key)
    document = collection.find_one({time_table_record.PROCESS_NAME: key,
                                    base_model.TIMEPERIOD: timeperiod})

    if document is None:
        raise LookupError('MongoDB has no time table record in %s collection for (%s, %s)' %
                          (collection, key, timeperiod))
    return TimeTableRecord(document)


#@thread_safe
def get_all(logger, table_name):
    """ method returns all time table records from particular table """
    query = {}
    ds = ds_manager.ds_factory(logger)
    collection = ds.connection(table_name)

    cursor = collection.find(query)
    if cursor.count() == 0:
        raise LookupError('MongoDB has no time table records in %s collection' % table_name)
    return [TimeTableRecord(document) for document in cursor]


#@thread_safe
def run_query(logger, collection_name, query):
    """ method runs query on specified table and return list of filtered TimeTableRecords """
    ds = ds_manager.ds_factory(logger)
    cursor = ds.filter(collection_name, query)
    return [TimeTableRecord(document) for document in cursor]


#@thread_safe
def update(logger, instance):
    collection = _get_timetable_collection(logger, instance.process_name)
    return collection.save(instance.document, safe=True)
