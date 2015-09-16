__author__ = 'Bohdan Mushkevych'

import collections
from threading import Lock
from datetime import datetime, timedelta

from synergy.conf import settings
from synergy.system.data_logging import get_logger
from synergy.system.decorator import thread_safe
from synergy.system.priority_queue import PriorityEntry, PriorityQueue, compute_release_time
from synergy.system.repeat_timer import RepeatTimer
from synergy.scheduler.scheduler_constants import QUEUE_UOW_REPORT, PROCESS_GC
from synergy.scheduler.thread_handler import ManagedThreadHandler
from synergy.db.model import unit_of_work
from synergy.db.model.synergy_mq_transmission import SynergyMqTransmission
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao


class GarbageCollector(object):
    """ GC is triggered directly by Synergy Scheduler.
        It scans for invalid or stalled unit_of_work and re-triggers them.
        GC is vital for the health of the system.
        Deployment with no GC is considered invalid """

    def __init__(self, scheduler):
        self.logger = get_logger(PROCESS_GC, append_to_console=False, redirect_stdstream=False)
        self.managed_handlers = scheduler.managed_handlers
        self.publishers = scheduler.publishers
        self.timetable = scheduler.timetable

        self.lock = Lock()
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.reprocess_uows = collections.defaultdict(PriorityQueue)
        self.timer = RepeatTimer(settings.settings['gc_run_interval'], self._run)

    @thread_safe
    def scan_uow_candidates(self):
        """ method performs two actions:
            - enlist stale or invalid units of work into reprocessing queue
            - cancel UOWs that are older than 2 days and have been submitted more than 1 hour ago """
        try:
            since = settings.settings['synergy_start_timeperiod']
            uow_list = self.uow_dao.get_reprocessing_candidates(since)
        except LookupError as e:
            self.logger.info('flow: no UOW candidates found for reprocessing: {0}'.format(e))
            return

        for uow in uow_list:
            try:
                if uow.process_name not in self.managed_handlers:
                    self.logger.debug('process {0} is not known to the Synergy Scheduler. Skipping its UOW.'
                                      .format(uow.process_name))
                    continue

                thread_handler = self.managed_handlers[uow.process_name]
                assert isinstance(thread_handler, ManagedThreadHandler)

                if not thread_handler.process_entry.is_on:
                    self.logger.debug('process {0} is inactive. Skipping its UOW.'.format(uow.process_name))
                    continue

                entry = PriorityEntry(uow)
                if entry in self.reprocess_uows[uow.process_name]:
                    # given UOW is already registered in the reprocessing queue
                    continue

                # ASSUMPTION: UOW is re-created by a state machine during reprocessing
                # thus - any UOW older 2 days could be marked as STATE_CANCELED
                if datetime.utcnow() - uow.created_at > timedelta(hours=settings.settings['gc_life_support_hours']):
                    self._cancel_uow(uow)
                    continue

                # if the UOW has been idle for more than 1 hour - resubmit it
                if datetime.utcnow() - uow.submitted_at > timedelta(hours=settings.settings['gc_resubmit_after_hours'])\
                        or uow.is_invalid:
                    # enlist the UOW into the reprocessing queue
                    self.reprocess_uows[uow.process_name].put(entry)

            except Exception as e:
                self.logger.error('flow exception: {0}'.format(e), exc_info=True)

    def _flush_queue(self, q, ignore_priority=False):
        """
        :param q: PriorityQueue instance holding GarbageCollector entries
        :param ignore_priority: If True - all GarbageCollector entries should be resubmitted
                If False - only those entries whose waiting time has expired will be resubmitted
        """
        assert isinstance(q, PriorityQueue)

        current_timestamp = compute_release_time(lag_in_minutes=0)
        for _ in range(len(q)):
            entry = q.pop()
            assert isinstance(entry, PriorityEntry)

            if ignore_priority or entry.release_time < current_timestamp:
                self._resubmit_uow(entry.entry)
            else:
                q.put(entry)
                break

    @thread_safe
    def flush(self, ignore_priority=False):
        """ method iterates over each reprocessing queues and re-submits UOW whose waiting time has expired """
        for process_name, q in self.reprocess_uows.items():
            self._flush_queue(q, ignore_priority)

    @thread_safe
    def flush_one(self, process_name, ignore_priority=False):
        """ method iterates over the reprocessing queue for the given process
            and re-submits UOW whose waiting time has expired """
        q = self.reprocess_uows[process_name]
        self._flush_queue(q, ignore_priority)

    def _resubmit_uow(self, uow):
        mq_request = SynergyMqTransmission(process_name=uow.process_name, unit_of_work_id=uow.db_id)

        if uow.is_invalid:
            uow.number_of_retries += 1

        uow.state = unit_of_work.STATE_REQUESTED
        uow.submitted_at = datetime.utcnow()
        self.uow_dao.update(uow)

        publisher = self.publishers.get(uow.process_name)
        publisher.publish(mq_request.document)
        publisher.release()

        self.logger.info('re-submitted UOW {0} for {1}@{2}; attempt {3}'
                         .format(uow.db_id, uow.process_name, uow.timeperiod, uow.number_of_retries))

    def _cancel_uow(self, uow):
        mq_request = SynergyMqTransmission(process_name=uow.process_name, unit_of_work_id=uow.db_id)

        uow.state = unit_of_work.STATE_CANCELED
        self.uow_dao.update(uow)

        publisher = self.publishers.get(QUEUE_UOW_REPORT)
        publisher.publish(mq_request.document)
        publisher.release()

        self.logger.info('canceled UOW {0} for {1}@{2}; attempt {3}; created at {4}'
                         .format(uow.db_id, uow.process_name, uow.timeperiod, uow.number_of_retries, uow.created_at))

    def _run(self):
        try:
            self.logger.info('run {')

            self.logger.debug('step 1: scan reprocessing candidates')
            self.scan_uow_candidates()

            self.logger.debug('step 2: repost after timeout')
            self.flush()

            self.logger.debug('step 3: timetable housekeeping')
            self.timetable.build_trees()

            self.logger.debug('step 4: timetable validation')
            self.timetable.validate()
        except Exception as e:
            self.logger.error('GC run exception: {0}'.format(e))
        finally:
            self.logger.info('}')

    def start(self):
        self.timer.start()

    def stop(self):
        self.timer.cancel()
