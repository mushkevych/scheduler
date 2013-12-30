__author__ = 'Bohdan Mushkevych'

from threading import RLock
from db.manager import ds_manager
from db.model import base_model, time_table_record
from db.model.time_table_record import TimeTableRecord
from system.decorator import thread_safe
from system.collection_context import COLLECTION_TIMETABLE_HOURLY, COLLECTION_TIMETABLE_DAILY, \
    COLLECTION_TIMETABLE_MONTHLY, COLLECTION_TIMETABLE_YEARLY
from system.process_context import ProcessContext


QUERY_GET_LIKE_TIMEPERIOD = \
    lambda timeperiod: {base_model.TIMEPERIOD: {'$regex': timeperiod}}

QUERY_GET_LIKE_TIMEPERIOD_AND_NOT_PROCESSED = \
    lambda timeperiod: {base_model.TIMEPERIOD: {'$regex': timeperiod},
                        time_table_record.STATE: {'$ne': time_table_record.STATE_PROCESSED}}


class TimeTableRecordDao(object):
    """ Thread-safe Data Access Object for box_configuration table/collection """
    def __init__(self, logger):
        super(TimeTableRecordDao, self).__init__()
        self.logger = logger
        self.lock = RLock()
        self.ds = ds_manager.ds_factory(logger)

    @thread_safe
    def _get_timetable_collection(self, process_name):
        """timetable stores timeperiod in 4 collections: hourly, daily, monthly and yearly;
        method looks for the proper timetable_collection base on process TIME_QUALIFIER"""
        qualifier = ProcessContext.get_time_qualifier(process_name)

        if qualifier == ProcessContext.QUALIFIER_HOURLY:
            collection = self.ds.connection(COLLECTION_TIMETABLE_HOURLY)
        elif qualifier == ProcessContext.QUALIFIER_DAILY:
            collection = self.ds.connection(COLLECTION_TIMETABLE_DAILY)
        elif qualifier == ProcessContext.QUALIFIER_MONTHLY:
            collection = self.ds.connection(COLLECTION_TIMETABLE_MONTHLY)
        elif qualifier == ProcessContext.QUALIFIER_YEARLY:
            collection = self.ds.connection(COLLECTION_TIMETABLE_YEARLY)
        else:
            raise ValueError('Unknown time qualifier: %s for %s' % (qualifier, process_name))
        return collection

    @thread_safe
    def get_one(self, key, timeperiod):
        """ method finds time_table record and returns it to the caller"""
        collection = self._get_timetable_collection(key)
        document = collection.find_one({time_table_record.PROCESS_NAME: key,
                                        base_model.TIMEPERIOD: timeperiod})

        if document is None:
            raise LookupError('MongoDB has no time table record in %s collection for (%s, %s)' %
                              (collection, key, timeperiod))
        return TimeTableRecord(document)

    @thread_safe
    def get_all(self, table_name, since=None):
        """ method returns all time table records from particular table that are older than <since> """
        if since is None:
            query = {}
        else:
            query = {base_model.TIMEPERIOD: {'$gte': since}}
        collection = self.ds.connection(table_name)

        cursor = collection.find(query)
        if cursor.count() == 0:
            raise LookupError('MongoDB has no time table records in %s collection since %r' % (table_name, since))
        return [TimeTableRecord(document) for document in cursor]

    @thread_safe
    def run_query(self, collection_name, query):
        """ method runs query on specified table and return list of filtered TimeTableRecords """
        cursor = self.ds.filter(collection_name, query)
        return [TimeTableRecord(document) for document in cursor]

    @thread_safe
    def update(self, instance):
        collection = self._get_timetable_collection(instance.process_name)
        return collection.save(instance.document, safe=True)
