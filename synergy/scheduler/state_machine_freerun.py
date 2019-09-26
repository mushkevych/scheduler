__author__ = 'Bohdan Mushkevych'

from datetime import datetime
from logging import ERROR, WARNING, INFO

from synergy.conf import context
from synergy.db.dao.freerun_process_dao import FreerunProcessDao
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.error import DuplicateKeyError
from synergy.db.model import unit_of_work
from synergy.db.model.freerun_process_entry import FreerunProcessEntry, MAX_NUMBER_OF_EVENTS
from synergy.db.model.unit_of_work import UnitOfWork
from synergy.scheduler.scheduler_constants import STATE_MACHINE_FREERUN
from synergy.system import time_helper
from synergy.system.decorator import with_reconnect
from synergy.system.mq_transmitter import MqTransmitter
from synergy.system.time_qualifier import QUALIFIER_REAL_TIME


class StateMachineFreerun(object):
    """ State Machine to handle freerun jobs/triggers """

    def __init__(self, logger, name=STATE_MACHINE_FREERUN):
        self.name = name
        self.logger = logger
        self.mq_transmitter = MqTransmitter(self.logger)
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.freerun_process_dao = FreerunProcessDao(self.logger)

    @with_reconnect
    def _log_message(self, level, freerun_entry, msg):
        """ method performs logging into log file and the freerun_entry """
        self.logger.log(level, msg)

        assert isinstance(freerun_entry, FreerunProcessEntry)
        event_log = freerun_entry.event_log
        if len(event_log) > MAX_NUMBER_OF_EVENTS:
            del event_log[-1]
        event_log.insert(0, msg)
        self.freerun_process_dao.update(freerun_entry)

    @with_reconnect
    def _insert_uow(self, freerun_entry, flow_request=None):
        """ creates unit_of_work and inserts it into the DB
            :raise DuplicateKeyError: if unit_of_work with given parameters already exists """
        process_entry = context.process_context[freerun_entry.process_name]
        arguments = process_entry.arguments
        arguments.update(freerun_entry.arguments)

        if flow_request:
            schedulable_name = flow_request.schedulable_name
            timeperiod = flow_request.timeperiod
            start_timeperiod = flow_request.start_timeperiod
            end_timeperiod = flow_request.end_timeperiod
            arguments.update(flow_request.arguments)
        else:
            schedulable_name = freerun_entry.schedulable_name
            timeperiod = time_helper.actual_timeperiod(QUALIFIER_REAL_TIME)
            start_timeperiod = timeperiod
            end_timeperiod = timeperiod

        uow = UnitOfWork()
        uow.process_name = schedulable_name
        uow.timeperiod = timeperiod
        uow.start_id = 0
        uow.end_id = 0
        uow.start_timeperiod = start_timeperiod
        uow.end_timeperiod = end_timeperiod
        uow.created_at = datetime.utcnow()
        uow.submitted_at = datetime.utcnow()
        uow.source = process_entry.source if hasattr(process_entry, 'source') else None
        uow.sink = process_entry.sink if hasattr(process_entry, 'sink') else None
        uow.state = unit_of_work.STATE_REQUESTED
        uow.unit_of_work_type = unit_of_work.TYPE_FREERUN
        uow.number_of_retries = 0
        uow.arguments = arguments
        uow.db_id = self.uow_dao.insert(uow)

        msg = 'Created: UOW {0} for {1}@{2}.' \
              .format(uow.db_id, freerun_entry.schedulable_name, timeperiod)
        self._log_message(INFO, freerun_entry, msg)
        return uow

    def _publish_uow(self, freerun_entry, uow):
        self.mq_transmitter.publish_freerun_uow(freerun_entry, uow)
        msg = f'Published: UOW {uow.db_id} for {freerun_entry.schedulable_name}.'
        self._log_message(INFO, freerun_entry, msg)

    def _reset_flow_uow(self, freerun_entry, uow, flow_request):
        """ there can be multiple freeruns for a single combination of workflow+step+timeperiod
            hence, we have to *recycle* finished UOW """
        process_entry = context.process_context[freerun_entry.process_name]
        arguments = process_entry.arguments
        arguments.update(freerun_entry.arguments)
        arguments.update(flow_request.arguments)

        uow.created_at = datetime.utcnow()
        uow.submitted_at = datetime.utcnow()
        uow.state = unit_of_work.STATE_REQUESTED
        uow.unit_of_work_type = unit_of_work.TYPE_FREERUN
        del uow.started_at
        del uow.finished_at
        del uow.number_of_aggregated_documents
        del uow.number_of_processed_documents
        uow.number_of_retries = 0
        uow.arguments = arguments
        self.uow_dao.update(uow)

    def insert_and_publish_uow(self, freerun_entry, flow_request=None, reset_uow=False):
        try:
            uow = self._insert_uow(freerun_entry, flow_request)
        except DuplicateKeyError as e:
            msg = f'Duplication of UOW found for {freerun_entry.schedulable_name}. Error msg: {e}'
            self._log_message(WARNING, freerun_entry, msg)
            uow = self.uow_dao.recover_from_duplicatekeyerror(e)

        if flow_request and reset_uow:
            self._reset_flow_uow(freerun_entry, uow, flow_request)

        if uow is not None:
            # publish the created/recovered/recycled unit_of_work
            self._publish_uow(freerun_entry, uow)
            freerun_entry.related_unit_of_work = uow.db_id

            if not flow_request:
                # FreerunProcessEntry for workflows are runtime-only objects
                # skip persistence update if this is a workflow request
                self.freerun_process_dao.update(freerun_entry)
        else:
            msg = f'PERSISTENT TIER ERROR! Unable to locate UOW for {freerun_entry.schedulable_name}'
            self._log_message(WARNING, freerun_entry, msg)

    def _process_state_embryo(self, freerun_entry, flow_request=None):
        """ method creates unit_of_work and associates it with the FreerunProcessEntry """
        self.insert_and_publish_uow(freerun_entry, flow_request)

    def _process_state_in_progress(self, freerun_entry, uow):
        """ method that takes care of processing unit_of_work records in STATE_REQUESTED or STATE_IN_PROGRESS states"""
        self._publish_uow(freerun_entry, uow)

    def _process_terminal_state(self, freerun_entry, uow, flow_request=None):
        """ method that takes care of processing unit_of_work records in
            STATE_PROCESSED, STATE_NOOP, STATE_INVALID, STATE_CANCELED states"""
        msg = f'UOW for {freerun_entry.schedulable_name} found in state {uow.state}.'
        self._log_message(INFO, freerun_entry, msg)
        self.insert_and_publish_uow(freerun_entry, flow_request, reset_uow=True)

    def manage_schedulable(self, freerun_entry: FreerunProcessEntry, flow_request=None):
        """ method main duty - is to _avoid_ publishing another unit_of_work, if previous was not yet processed
            In case the Scheduler sees that the unit_of_work is pending,
            it will issue new WorkerMqRequest for the same UOW """

        uow = None
        if freerun_entry.related_unit_of_work:
            uow = self.uow_dao.get_one(freerun_entry.related_unit_of_work)

        try:
            if uow is None:
                self._process_state_embryo(freerun_entry, flow_request)

            elif uow.is_requested or uow.is_in_progress:
                self._process_state_in_progress(freerun_entry, uow)

            elif uow.is_finished or uow.is_invalid:
                self._process_terminal_state(freerun_entry, uow, flow_request)

            else:
                msg = f'Unknown state {uow.state} of the UOW {uow.db_id}'
                self._log_message(ERROR, freerun_entry, msg)

        except LookupError as e:
            msg = f'Lookup issue for schedulable: {freerun_entry.db_id} in timeperiod {uow.timeperiod}, because of: {e}'
            self._log_message(WARNING, freerun_entry, msg)

    def cancel_uow(self, freerun_entry):
        uow_id = freerun_entry.related_unit_of_work
        if uow_id is None:
            msg = f'cancel_uow: no related UOW for {freerun_entry.schedulable_name}'
        else:
            uow = self.uow_dao.get_one(uow_id)
            uow.state = unit_of_work.STATE_CANCELED
            self.uow_dao.update(uow)
            msg = f'cancel_uow: canceled UOW {uow_id} for {freerun_entry.schedulable_name}'
        self._log_message(INFO, freerun_entry, msg)
