__author__ = 'Bohdan Mushkevych'

from threading import RLock
from db.dao import time_table_record_dao
from db.dao.time_table_record_dao import TimeTableRecordDao
from system.collection_context import COLLECTION_TIMETABLE_YEARLY, \
    COLLECTION_TIMETABLE_MONTHLY, COLLECTION_TIMETABLE_DAILY, COLLECTION_TIMETABLE_HOURLY
from system.decorator import thread_safe


class ProcessingStatements(object):
    """ Reads from DB status of timeperiods and their processing status """

    def __init__(self, logger):
        self.lock = RLock()
        self.logger = logger
        self.ttr_dao = TimeTableRecordDao(self.logger)

    @thread_safe
    def retrieve_for_timeperiod(self, timeperiod, unprocessed_only):
        """ method iterates thru all objects in timetable collections and load them into timetable"""
        resp = dict()
        resp.update(self._search_by_level(COLLECTION_TIMETABLE_HOURLY, timeperiod, unprocessed_only))
        resp.update(self._search_by_level(COLLECTION_TIMETABLE_DAILY, timeperiod, unprocessed_only))
        resp.update(self._search_by_level(COLLECTION_TIMETABLE_MONTHLY, timeperiod, unprocessed_only))
        resp.update(self._search_by_level(COLLECTION_TIMETABLE_YEARLY, timeperiod, unprocessed_only))
        return resp

    @thread_safe
    def _search_by_level(self, collection_name, timeperiod, unprocessed_only):
        """ method iterated thru all documents in all timetable collections and builds tree of known system state"""
        resp = dict()
        try:
            if unprocessed_only:
                query = time_table_record_dao.QUERY_GET_LIKE_TIMEPERIOD_AND_NOT_PROCESSED(timeperiod)
            else:
                query = time_table_record_dao.QUERY_GET_LIKE_TIMEPERIOD(timeperiod)

            time_record_list = self.ttr_dao.run_query(collection_name, query)
            if len(time_record_list) == 0:
                self.logger.warning('No TimeTable Records in %s.' % str(collection_name))

            for time_table_rec in time_record_list:
                resp[time_table_rec.key] = time_table_rec
        except Exception as e:
            self.logger.error('ProcessingStatements error: %s' % str(e))
        return resp


if __name__ == '__main__':
    import logging

    pd = ProcessingStatements(logging)
    resp = pd.retrieve_for_timeperiod('201110', False)
    print('%r' % resp)
