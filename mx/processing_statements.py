__author__ = 'Bohdan Mushkevych'

from threading import RLock

from werkzeug.utils import cached_property

from db.dao import time_table_record_dao
from db.dao.time_table_record_dao import TimeTableRecordDao
from system.collection_context import COLLECTION_TIMETABLE_YEARLY, \
    COLLECTION_TIMETABLE_MONTHLY, COLLECTION_TIMETABLE_DAILY, COLLECTION_TIMETABLE_HOURLY
from system.decorator import thread_safe
from mx.commons import managed_entry_request


class ProcessingStatementDetails(object):
    def __init__(self, mbean, request):
        self.mbean = mbean
        self.logger = self.mbean.logger
        self.request = request
        self.year = self.request.args.get('year')
        self.month = self.request.args.get('month')
        self.day = self.request.args.get('day')
        self.hour = self.request.args.get('hour')
        self.state = self.request.args.get('state')
        if self.state is not None and self.state == 'on':
            self.state = True
        else:
            self.state = False

        if self.year is not None and self.year.strip() == '':
            self.year = None
        if self.month is not None and self.month.strip() == '':
            self.month = None
        if self.day is not None and self.day.strip() == '':
            self.day = None
        self.is_managed_request_valid = self.mbean is not None \
                                        and self.year is not None \
                                        and self.month is not None \
                                        and self.day is not None \
                                        and self.hour is not None

    @cached_property
    @managed_entry_request
    def entries(self):
        processor = ProcessingStatements(self.logger)
        timeperiod = self.year + self.month + self.day + self.hour
        selection = processor.retrieve_for_timeperiod(timeperiod, self.state)
        sorter_keys = sorted(selection.keys())

        resp = []
        for key in sorter_keys:
            t = (key[0], key[1], selection[key].state)
            resp.append(t)

        print ('%r' % resp)
        return resp


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

            tt_record_list = self.ttr_dao.run_query(collection_name, query)
            if len(tt_record_list) == 0:
                self.logger.warning('No TimeTable Records in %s.' % str(collection_name))

            for timetable_record in tt_record_list:
                resp[timetable_record.key] = timetable_record
        except Exception as e:
            self.logger.error('ProcessingStatements error: %s' % str(e))
        return resp


if __name__ == '__main__':
    import logging

    pd = ProcessingStatements(logging)
    resp = pd.retrieve_for_timeperiod('201110', False)
    print('%r' % resp)
