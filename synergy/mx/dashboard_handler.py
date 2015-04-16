__author__ = 'Bohdan Mushkevych'

from collections import OrderedDict
from threading import RLock

from werkzeug.utils import cached_property

from synergy.db.dao import job_dao
from synergy.db.dao.job_dao import JobDao
from synergy.db.dao import unit_of_work_dao
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.scheduler.scheduler_constants import COLLECTION_JOB_YEARLY, \
    COLLECTION_JOB_MONTHLY, COLLECTION_JOB_DAILY, COLLECTION_JOB_HOURLY
from synergy.system.decorator import thread_safe
from synergy.system import time_helper
from synergy.system.time_qualifier import QUALIFIER_DAILY, QUALIFIER_MONTHLY, QUALIFIER_YEARLY
from synergy.mx.base_request_handler import BaseRequestHandler, valid_action_request

TIME_WINDOW_DAY_PREFIX = 'day_'


class DashboardHandler(BaseRequestHandler):
    def __init__(self, request, **values):
        super(DashboardHandler, self).__init__(request, **values)

        self.time_window = self.request.args.get('time_window')
        self.is_unprocessed_only = self.request.args.get('unprocessed_only') == 'on'
        if self.time_window:
            self.is_request_valid = True
        else:
            self.is_request_valid = False

    @cached_property
    @valid_action_request
    def managed(self):
        processor = ManagedStatements(self.logger)
        actual_timeperiod = time_helper.actual_timeperiod(QUALIFIER_DAILY)
        delta = int(self.time_window[len(TIME_WINDOW_DAY_PREFIX) + 1:])
        start_timeperiod = time_helper.increment_timeperiod(QUALIFIER_DAILY, actual_timeperiod, -delta)

        selection = processor.retrieve_records(start_timeperiod, self.is_unprocessed_only)
        return OrderedDict(sorted(selection.items()))

    @cached_property
    @valid_action_request
    def freeruns(self):
        processor = FreerunStatements(self.logger)
        actual_timeperiod = time_helper.actual_timeperiod(QUALIFIER_DAILY)
        delta = int(self.time_window[len(TIME_WINDOW_DAY_PREFIX) + 1:])
        start_timeperiod = time_helper.increment_timeperiod(QUALIFIER_DAILY, actual_timeperiod, -delta)

        selection = processor.retrieve_records(start_timeperiod, self.is_unprocessed_only)
        return OrderedDict(sorted(selection.items()))


class ManagedStatements(object):
    def __init__(self, logger):
        self.lock = RLock()
        self.logger = logger
        self.job_dao = JobDao(self.logger)

    @thread_safe
    def retrieve_records(self, timeperiod, is_unprocessed_only):
        """ method looks for suitable job records in all Job collections and returns them as a dict"""
        resp = dict()
        resp.update(self._search_by_level(COLLECTION_JOB_HOURLY, timeperiod, is_unprocessed_only))
        resp.update(self._search_by_level(COLLECTION_JOB_DAILY, timeperiod, is_unprocessed_only))

        timeperiod = time_helper.cast_to_time_qualifier(QUALIFIER_MONTHLY, timeperiod)
        resp.update(self._search_by_level(COLLECTION_JOB_MONTHLY, timeperiod, is_unprocessed_only))

        timeperiod = time_helper.cast_to_time_qualifier(QUALIFIER_YEARLY, timeperiod)
        resp.update(self._search_by_level(COLLECTION_JOB_YEARLY, timeperiod, is_unprocessed_only))
        return resp

    @thread_safe
    def _search_by_level(self, collection_name, timeperiod, unprocessed_only):
        resp = dict()
        try:
            query = job_dao.QUERY_GET_LIKE_TIMEPERIOD(timeperiod, unprocessed_only)
            records_list = self.job_dao.run_query(collection_name, query)
            if len(records_list) == 0:
                self.logger.warn('No Job Records found in {0} since {1}.'.format(collection_name, timeperiod))

            for job_record in records_list:
                resp[job_record.key] = job_record.document
        except Exception as e:
            self.logger.error('DashboardHandler error: {0}'.format(e))
        return resp


class FreerunStatements(object):
    def __init__(self, logger):
        self.lock = RLock()
        self.logger = logger
        self.uow_dao = UnitOfWorkDao(self.logger)

    @thread_safe
    def retrieve_records(self, timeperiod, unprocessed_only):
        """ method looks for suitable UOW records and returns them as a dict"""
        resp = dict()
        try:
            query = unit_of_work_dao.QUERY_GET_FREERUN_SINCE(timeperiod, unprocessed_only)
            records_list = self.uow_dao.run_query(query)
            if len(records_list) == 0:
                self.logger.warn('No Freerun UOW records found since {0}.'.format(timeperiod))

            for uow_record in records_list:
                resp[uow_record.key] = uow_record.document
        except Exception as e:
            self.logger.error('DashboardHandler error: {0}'.format(e))
        return resp


if __name__ == '__main__':
    import logging

    for pd in [ManagedStatements(logging), FreerunStatements(logging)]:
        resp = pd.retrieve_records('2015030100', False)
        print('{0}'.format(resp))
