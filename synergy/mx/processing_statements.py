__author__ = 'Bohdan Mushkevych'

from threading import RLock

from werkzeug.utils import cached_property

from synergy.db.dao import job_dao
from synergy.db.dao.job_dao import JobDao
from synergy.scheduler.scheduler_constants import COLLECTION_JOB_YEARLY, \
    COLLECTION_JOB_MONTHLY, COLLECTION_JOB_DAILY, COLLECTION_JOB_HOURLY
from synergy.system.decorator import thread_safe
from synergy.mx.mx_decorators import valid_action_request


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
        self.is_request_valid = self.mbean is not None \
                                and not not self.year \
                                and not not self.month \
                                and not not self.day

    @cached_property
    @valid_action_request
    def entries(self):
        processor = ProcessingStatements(self.logger)
        timeperiod = self.year + self.month + self.day + self.hour
        selection = processor.retrieve_for_timeperiod(timeperiod, self.state)
        sorter_keys = sorted(selection.keys())

        resp = []
        for key in sorter_keys:
            t = (key[0], key[1], selection[key].state)
            resp.append(t)
        return resp


class ProcessingStatements(object):
    """ Reads from DB status of timeperiods and their processing status """

    def __init__(self, logger):
        self.lock = RLock()
        self.logger = logger
        self.job_dao = JobDao(self.logger)

    @thread_safe
    def retrieve_for_timeperiod(self, timeperiod, unprocessed_only):
        """ method iterates thru all objects in job collections and load them into a dict"""
        resp = dict()
        resp.update(self._search_by_level(COLLECTION_JOB_HOURLY, timeperiod, unprocessed_only))
        resp.update(self._search_by_level(COLLECTION_JOB_DAILY, timeperiod, unprocessed_only))
        resp.update(self._search_by_level(COLLECTION_JOB_MONTHLY, timeperiod, unprocessed_only))
        resp.update(self._search_by_level(COLLECTION_JOB_YEARLY, timeperiod, unprocessed_only))
        return resp

    @thread_safe
    def _search_by_level(self, collection_name, timeperiod, unprocessed_only):
        """ method iterated thru all documents in all job collections and builds tree of known system state"""
        resp = dict()
        try:
            if unprocessed_only:
                query = job_dao.QUERY_GET_LIKE_TIMEPERIOD_AND_NOT_PROCESSED(timeperiod)
            else:
                query = job_dao.QUERY_GET_LIKE_TIMEPERIOD(timeperiod)

            job_record_list = self.job_dao.run_query(collection_name, query)
            if len(job_record_list) == 0:
                self.logger.warning('No Job Records in %s.' % collection_name)

            for job_record in job_record_list:
                resp[job_record.key] = job_record
        except Exception as e:
            self.logger.error('ProcessingStatements error: %r' % e)
        return resp


if __name__ == '__main__':
    import logging

    pd = ProcessingStatements(logging)
    resp = pd.retrieve_for_timeperiod('201110', False)
    print('%r' % resp)
