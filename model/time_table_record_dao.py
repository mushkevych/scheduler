__author__ = 'Bohdan Mushkevych'

from model import time_table_record, base_model
from model.time_table_record import TimeTableRecord
from system.collection_context import CollectionContext, COLLECTION_TIMETABLE_HOURLY, COLLECTION_TIMETABLE_DAILY, \
    COLLECTION_TIMETABLE_MONTHLY, COLLECTION_TIMETABLE_YEARLY
from system.process_context import ProcessContext


#@thread_safe
def _get_timetable_collection(logger, process_name):
    """timetable stores timeperiod in 4 collections: hourly, daily, monthly and yearly; method looks for the
    proper timetable_collection base on process TIME_QUALIFIER"""
    qualifier = ProcessContext.get_time_qualifier(process_name)
    if qualifier == ProcessContext.QUALIFIER_HOURLY:
        collection = CollectionContext.get_collection(logger, COLLECTION_TIMETABLE_HOURLY)
    elif qualifier == ProcessContext.QUALIFIER_DAILY:
        collection = CollectionContext.get_collection(logger, COLLECTION_TIMETABLE_DAILY)
    elif qualifier == ProcessContext.QUALIFIER_MONTHLY:
        collection = CollectionContext.get_collection(logger, COLLECTION_TIMETABLE_MONTHLY)
    elif qualifier == ProcessContext.QUALIFIER_YEARLY:
        collection = CollectionContext.get_collection(logger, COLLECTION_TIMETABLE_YEARLY)
    else:
        raise ValueError('unknown time qualifier: %s for %s' % (qualifier, process_name))
    return collection


def get_one(logger, key, timeperiod):
    """ method finds time_table record and returns it to the caller"""

    collection = _get_timetable_collection(logger, key)
    document = collection.find_one({time_table_record.PROCESS_NAME: key,
                                    base_model.TIMEPERIOD: timeperiod})

    if document is None:
        return None
    return TimeTableRecord(document)


def get_all(logger, table_name):
    query = {}
    collection = CollectionContext.get_collection(logger, table_name)
    cursor = collection.find(query)
    if cursor.count() == 0:
        raise LookupError('MongoDB has no time table records in %s collection' % table_name)
    return [TimeTableRecord(entry) for entry in cursor]


#@thread_safe
def update(logger, instance):
    collection = _get_timetable_collection(logger, instance.process_name)
    return collection.save(instance.document, safe=True)
