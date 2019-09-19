__author__ = 'Bohdan Mushkevych'

from collections import OrderedDict
from threading import RLock

from werkzeug.utils import cached_property

from synergy.db.dao import job_dao
from synergy.db.dao.job_dao import JobDao
from synergy.db.dao import unit_of_work_dao
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.model.freerun_process_entry import split_schedulable_name
from synergy.scheduler.scheduler_constants import COLLECTION_JOB_YEARLY, \
    COLLECTION_JOB_MONTHLY, COLLECTION_JOB_DAILY, COLLECTION_JOB_HOURLY
from synergy.system.decorator import thread_safe
from synergy.system import time_helper
from synergy.system.time_qualifier import QUALIFIER_DAILY, QUALIFIER_MONTHLY, QUALIFIER_YEARLY
from synergy.mx.base_request_handler import BaseRequestHandler, valid_action_request


class DashboardHandler(BaseRequestHandler):
    def __init__(self, request, **values):
        super(DashboardHandler, self).__init__(request, **values)

        self.time_window = self.request.args.get('time_window')
        self.is_include_running = self.request.args.get('include_running') == 'on'
        self.is_include_processed = self.request.args.get('include_processed') == 'on'
        self.is_include_noop = self.request.args.get('include_noop') == 'on'
        self.is_include_failed = self.request.args.get('include_failed') == 'on'
        self.is_include_disabled = self.request.args.get('include_disabled') == 'on'
        self.is_request_valid = bool(self.time_window)

        if self.is_request_valid:
            actual_timeperiod = time_helper.actual_timeperiod(QUALIFIER_DAILY)
            delta = int(self.time_window)
            self.query_start_timeperiod = time_helper.increment_timeperiod(QUALIFIER_DAILY, actual_timeperiod, -delta)

    @cached_property
    @valid_action_request
    def managed(self):
        processor = ManagedStatements(self.logger, self.scheduler.managed_handlers)
        selection = processor.retrieve_records(self.query_start_timeperiod, self.is_include_running,
                                               self.is_include_processed, self.is_include_noop, self.is_include_failed,
                                               self.is_include_disabled)
        return OrderedDict(sorted(selection.items()))

    @cached_property
    @valid_action_request
    def freeruns(self):
        processor = FreerunStatements(self.logger, self.scheduler.freerun_handlers)
        selection = processor.retrieve_records(self.query_start_timeperiod, self.is_include_running,
                                               self.is_include_processed, self.is_include_noop, self.is_include_failed,
                                               self.is_include_disabled)
        return OrderedDict(sorted(selection.items()))


class ManagedStatements(object):
    def __init__(self, logger, managed_handlers):
        self.lock = RLock()
        self.logger = logger
        self.managed_handlers = managed_handlers
        self.job_dao = JobDao(self.logger)

    @thread_safe
    def retrieve_records(self, timeperiod, include_running,
                         include_processed, include_noop, include_failed, include_disabled):
        """ method looks for suitable job records in all Job collections and returns them as a dict"""
        resp = dict()
        resp.update(self._search_by_level(COLLECTION_JOB_HOURLY, timeperiod, include_running,
                                          include_processed, include_noop, include_failed, include_disabled))
        resp.update(self._search_by_level(COLLECTION_JOB_DAILY, timeperiod, include_running,
                                          include_processed, include_noop, include_failed, include_disabled))

        timeperiod = time_helper.cast_to_time_qualifier(QUALIFIER_MONTHLY, timeperiod)
        resp.update(self._search_by_level(COLLECTION_JOB_MONTHLY, timeperiod, include_running,
                                          include_processed, include_noop, include_failed, include_disabled))

        timeperiod = time_helper.cast_to_time_qualifier(QUALIFIER_YEARLY, timeperiod)
        resp.update(self._search_by_level(COLLECTION_JOB_YEARLY, timeperiod, include_running,
                                          include_processed, include_noop, include_failed, include_disabled))
        return resp

    @thread_safe
    def _search_by_level(self, collection_name, timeperiod, include_running,
                         include_processed, include_noop, include_failed, include_disabled):
        resp = dict()
        try:
            query = job_dao.QUERY_GET_LIKE_TIMEPERIOD(timeperiod, include_running,
                                                      include_processed, include_noop, include_failed)
            records_list = self.job_dao.run_query(collection_name, query)
            if len(records_list) == 0:
                self.logger.warning(f'MX: no Job Records found in {collection_name} since {timeperiod}.')

            for job_record in records_list:
                if job_record.process_name not in self.managed_handlers:
                    continue

                thread_handler = self.managed_handlers[job_record.process_name]
                if not include_disabled and not thread_handler.process_entry.is_on:
                    continue

                resp[job_record.key] = job_record.document
        except Exception as e:
            self.logger.error(f'MX Dashboard ManagedStatements error: {e}')
        return resp


class FreerunStatements(object):
    def __init__(self, logger, freerun_handlers):
        self.lock = RLock()
        self.logger = logger
        self.freerun_handlers = freerun_handlers
        self.uow_dao = UnitOfWorkDao(self.logger)

    @thread_safe
    def retrieve_records(self, timeperiod, include_running,
                         include_processed, include_noop, include_failed, include_disabled):
        """ method looks for suitable UOW records and returns them as a dict"""
        resp = dict()
        try:
            query = unit_of_work_dao.QUERY_GET_FREERUN_SINCE(timeperiod, include_running,
                                                             include_processed, include_noop, include_failed)
            records_list = self.uow_dao.run_query(query)
            if len(records_list) == 0:
                self.logger.warning(f'MX: no Freerun UOW records found since {timeperiod}.')

            for uow_record in records_list:
                # freerun uow.process_name is a composite in format <process_name::entry_name>
                handler_key = split_schedulable_name(uow_record.process_name)
                if handler_key not in self.freerun_handlers:
                    continue

                thread_handler = self.freerun_handlers[handler_key]
                if not include_disabled and not thread_handler.process_entry.is_on:
                    continue

                resp[uow_record.key] = uow_record.document
        except Exception as e:
            self.logger.error(f'MX Dashboard FreerunStatements error: {e}')
        return resp
