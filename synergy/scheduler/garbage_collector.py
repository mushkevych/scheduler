from Queue import PriorityQueue
from db.model.unit_of_work import UnitOfWork

__author__ = 'Bohdan Mushkevych'

import collections
from threading import Lock
from datetime import datetime, timedelta

from synergy.conf import settings
from synergy.system import time_helper
from synergy.system.time_qualifier import QUALIFIER_REAL_TIME
from synergy.system.decorator import thread_safe
from synergy.scheduler.scheduler_constants import QUEUE_UOW_REPORT
from synergy.db.model import unit_of_work
from synergy.db.model.synergy_mq_transmission import SynergyMqTransmission
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.model.managed_process_entry import ManagedProcessEntry


class CollectorEntry(object):
    # Creation counter keeps track of CollectorEntry declaration order
    # Each time a CollectorEntry instance is created the counter should be increased
    creation_counter = 0

    def __init__(self, uow):
        """
        :param uow: the unit_of_work to reprocess
        :return:
        """
        self.uow = uow
        self.release_time = self._compute_release_time()  # time in the future in the SYNERGY_SESSION_PATTERN
        self.creation_counter = CollectorEntry.creation_counter + 1
        CollectorEntry.creation_counter += 1

    def _compute_release_time(self):
        future_dt = datetime.utcnow() + timedelta(minutes=settings.settings['gc_release_lag_minutes'])
        release_time_str = time_helper.datetime_to_synergy(QUALIFIER_REAL_TIME, future_dt)
        return int(release_time_str)

    def __eq__(self, other):
        return self.uow.db_id == other.uow.db_id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        """Defines behavior for the less-than operator, <."""
        if self.release_time == other.release_time:
            return self.creation_counter < other.creation_counter
        else:
            return self.release_time < other.release_time

    def __gt__(self, other):
        """Defines behavior for the greater-than operator, >."""
        if self.release_time == other.release_time:
            return self.creation_counter > other.creation_counter
        else:
            return self.release_time > other.release_time

    def __le__(self, other):
        """Defines behavior for the less-than-or-equal-to operator, <=."""
        if self.release_time == other.release_time:
            return self.creation_counter <= other.creation_counter
        else:
            return self.release_time <= other.release_time

    def __ge__(self, other):
        """Defines behavior for the greater-than-or-equal-to operator, >=."""
        if self.release_time == other.release_time:
            return self.creation_counter >= other.creation_counter
        else:
            return self.release_time >= other.release_time

    def __hash__(self):
        return hash((self.release_time, self.creation_counter))


class GarbageCollector(object):
    """ GC is triggered directly by Synergy Scheduler.
        It scans for invalid or stalled unit_of_work and re-triggers them.
        GC is vital for the health of the system.
        Deployment with no GC is considered invalid """

    def __init__(self, logger, managed_handlers, publishers):
        self.logger = logger
        self.managed_handlers = managed_handlers
        self.publishers = publishers

        self.lock = Lock()
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.reprocess_uows = collections.defaultdict(PriorityQueue)

    @thread_safe
    def enlist_or_cancel(self):
        """ method performs two actions:
            - enlist stale or invalid units of work into reprocessing queue
            - cancel UOWs that are older than 2 days and have been submitted more than 1 hour ago """
        try:
            since = settings.settings['synergy_start_timeperiod']
            uow_list = self.uow_dao.get_reprocessing_candidates(since)
            for uow in uow_list:
                if uow.process_name not in self.managed_handlers:
                    self.logger.debug('Process %r is not known to the Synergy Scheduler. Skipping its unit_of_work.'
                                      % uow.process_name)
                    continue

                process_entry = self.managed_handlers[uow.process_name]
                assert isinstance(process_entry, ManagedProcessEntry)
                if not process_entry.is_on:
                    self.logger.debug('Process %r is inactive. Skipping its unit_of_work.' % uow.process_name)
                    continue

                if uow in self.reprocess_uows[uow.process_name]:
                    # given UOW is already registered in the reprocessing queue
                    continue

                if datetime.utcnow() - uow.created_at > timedelta(hours=settings.settings['gc_life_support_hours']) \
                        and datetime.utcnow() - uow.submitted_at > timedelta(
                                                                  hours=settings.settings['gc_repost_after_hours']):
                    self._cancel_uow(uow)
                    continue

                # enlist the UOW into the reprocessing queue
                entry = CollectorEntry(uow)
                self.reprocess_uows[uow.process_name].put_nowait(entry)

        except LookupError as e:
            self.logger.info('Expected case: re-processing UOW candidates not found. %r' % e)
        except Exception as e:
            self.logger.error('GC Exception: %s' % str(e), exc_info=True)

    @thread_safe
    def repost(self):
        """ method iterates ove the reprocessing queue and re-submits UOW whose waiting time has expired """
        current_timestamp = time_helper.actual_timeperiod(QUALIFIER_REAL_TIME)
        for process_name, q in self.reprocess_uows.items():
            assert isinstance(q, PriorityQueue)
            for _ in range(q.qsize()):
                entry = q.get_nowait()
                assert isinstance(entry, CollectorEntry)

                if entry.release_time < current_timestamp:
                    self._resubmit_uow(entry.uow)
                else:
                    q.put_nowait(entry)
                    break   # leave the loop for given process_name

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

        self.logger.info('re-submitted UOW {0} for {1} in {2}; attempt {3}'
                         .format(uow.db_id, uow.process_name, uow.timeperiod, uow.number_of_retries))

    def _cancel_uow(self, uow):
        mq_request = SynergyMqTransmission(process_name=uow.process_name, unit_of_work_id=uow.db_id)

        uow.state = unit_of_work.STATE_CANCELED
        self.uow_dao.update(uow)

        publisher = self.publishers.get(QUEUE_UOW_REPORT)
        publisher.publish(mq_request.document)
        publisher.release()

        self.logger.info('canceled UOW {0} for {1} in {2}; attempt {3}; created at {4}'
                         .format(uow.db_id, uow.process_name, uow.timeperiod, uow.number_of_retries, uow.created_at))
