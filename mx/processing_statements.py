__author__ = 'Bohdan Mushkevych'

from threading import RLock
from model import base_model
from model import time_table
from model.time_table import TimeTable
from system.collection_context import CollectionContext, COLLECTION_TIMETABLE_YEARLY, \
    COLLECTION_TIMETABLE_MONTHLY, COLLECTION_TIMETABLE_DAILY, COLLECTION_TIMETABLE_HOURLY
from system.decorator import thread_safe


class ProcessingStatements(object):
    """ Reads from MongoDB status of timeperiods and their processing status """

    def __init__(self, logger):
        self.lock = RLock()
        self.logger = logger

    @thread_safe
    def retrieve_for_timeperiod(self, timeperiod, unprocessed_only):
        """ method iterates thru all objects in timetable collections and load them into timetable"""
        resp = dict()
        resp.update(self._search_by_level(CollectionContext.get_collection(self.logger, COLLECTION_TIMETABLE_HOURLY),
                                          timeperiod, unprocessed_only))
        resp.update(self._search_by_level(CollectionContext.get_collection(self.logger, COLLECTION_TIMETABLE_DAILY),
                                          timeperiod, unprocessed_only))
        resp.update(self._search_by_level(CollectionContext.get_collection(self.logger, COLLECTION_TIMETABLE_MONTHLY),
                                          timeperiod, unprocessed_only))
        resp.update(self._search_by_level(CollectionContext.get_collection(self.logger, COLLECTION_TIMETABLE_YEARLY),
                                          timeperiod, unprocessed_only))
        return resp

    @thread_safe
    def _search_by_level(self, collection, timeperiod, unprocessed_only):
        """ method iterated thru all documents in all timetable collections and builds tree of known system state"""
        resp = dict()
        try:
            if unprocessed_only:
                query = {base_model.TIMEPERIOD: {'$regex': timeperiod},
                         time_table.STATE: {'$ne': time_table.STATE_PROCESSED}}
            else:
                query = {base_model.TIMEPERIOD: {'$regex': timeperiod}}

            cursor = collection.find(query)
            if cursor.count() == 0:
                self.logger.warning('No TimeTable Records in %s.' % str(collection))
            else:
                for document in cursor:
                    obj = TimeTable(document)
                    key = (obj.process_name, obj.timeperiod)
                    resp[key] = obj
                    print(key)
        except Exception as e:
            self.logger.error('ProcessingStatements error: %s' % str(e))
        return resp


if __name__ == '__main__':
    import logging

    pd = ProcessingStatements(logging)
    resp = pd.retrieve_for_timeperiod('201110', False)
    print('%r' % resp)
