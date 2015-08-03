
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
from synergy.db.dao.managed_process_dao import ManagedProcessDao


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
        return self.uow.db_id == other.uow.db_id \
            and self.release_time == other.release_time \
            and self.creation_counter == other.creation_counter

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
        self.reprocess_uows = collections.defaultdict(dict)

    @thread_safe
    def run(self):
        """ method performs two actions:
            - updates list of stale or invalid units of work
            - re-publishes unit_of_work that are in the reprocess_uows queue whose  needed"""
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

                self._process_single_document(uow)

        except LookupError as e:
            self.logger.info('Expected case: re-processing UOW candidates not found. %r' % e)
        except Exception as e:
            self.logger.error('GC Exception: %s' % str(e), exc_info=True)

    def _process_single_document(self, uow):
        """  inspects UOW retrieved from the database"""
        enlist = False
        repost = False
        if uow.is_invalid:
            enlist = True

        if uow.is_in_progress or uow.is_requested:
            if datetime.utcnow() - uow.submitted_at > timedelta(hours=settings.settings['gc_repost_after_hours']):
                repost = True

        if repost:
            mq_request = SynergyMqTransmission(process_name=uow.process_name,
                                               unit_of_work_id=uow.db_id)

            if datetime.utcnow() - uow.submitted_at < timedelta(hours=settings.settings['gc_life_support_hours']):
                uow.state = unit_of_work.STATE_REQUESTED
                uow.number_of_retries += 1
                uow.submitted_at = datetime.utcnow()
                self.uow_dao.update(uow)

                publisher = self.publishers.get(uow.process_name)
                publisher.publish(mq_request.document)
                publisher.release()

                self.logger.info('UOW marked for re-processing: process %s; timeperiod %s; id %s; attempt %d'
                                 % (uow.process_name, uow.timeperiod, uow.db_id, uow.number_of_retries))
            else:
                uow.state = unit_of_work.STATE_CANCELED
                self.uow_dao.update(uow)

                publisher = self.publishers.get(QUEUE_UOW_REPORT)
                publisher.publish(mq_request.document)
                publisher.release()

                self.logger.info('UOW transferred to STATE_CANCELED: process %s; timeperiod %s; id %s; attempt %d'
                                 % (uow.process_name, uow.timeperiod, uow.db_id, uow.number_of_retries))


if __name__ == '__main__':
    from synergy.scheduler.scheduler_constants import PROCESS_GC, TYPE_MANAGED

    source = GarbageCollector(PROCESS_GC)
    source.start()
