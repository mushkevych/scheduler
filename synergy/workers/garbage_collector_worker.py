__author__ = 'Bohdan Mushkevych'

from threading import Lock
from datetime import datetime, timedelta

from synergy.conf import settings
from synergy.mq.flopsy import PublishersPool
from synergy.system.decorator import thread_safe
from synergy.scheduler.scheduler_constants import QUEUE_UOW_REPORT
from synergy.workers.abstract_mq_worker import AbstractMqWorker
from synergy.db.model import unit_of_work
from synergy.db.model.synergy_mq_transmission import SynergyMqTransmission
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.model.managed_process_entry import ManagedProcessEntry
from synergy.db.dao.managed_process_dao import ManagedProcessDao


LIFE_SUPPORT_HOURS = 48  # number of hours from UOW creation time to keep UOW re-posting to MQ
REPOST_AFTER_HOURS = 1   # number of hours, GC waits for the worker to pick up the UOW from MQ before re-posting


class GarbageCollectorWorker(AbstractMqWorker):
    """ GC is triggered by an empty message from RabbitMQ. It scans for invalid or stalled unit_of_work
    and re-triggers them. GC is vital for the health of the system.
    Deployment with no running GC is considered invalid """

    def __init__(self, process_name):
        super(GarbageCollectorWorker, self).__init__(process_name)
        self.lock = Lock()
        self.publishers = PublishersPool(self.logger)
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.managed_dao = ManagedProcessDao(self.logger)
        self.managed_entries = dict()

    def __del__(self):
        try:
            self.logger.info('Closing Flopsy Publishers Pool...')
            self.publishers.close()
        except Exception as e:
            self.logger.error('Exception caught while closing Flopsy Publishers Pool: %s' % str(e))

        super(GarbageCollectorWorker, self).__del__()

    @thread_safe
    def _mq_callback(self, message):
        """ method looks for stale or invalid units of work re-runs them if needed"""
        try:
            managed_entries = self.managed_dao.get_all()
            self._update_managed_entries(managed_entries)

            since = settings.settings['synergy_start_timeperiod']
            uow_list = self.uow_dao.get_reprocessing_candidates(since)
            for uow in uow_list:
                if uow.process_name not in self.managed_entries:
                    self.logger.debug('Process %r is not known to the Synergy Scheduler. Skipping its unit_of_work.'
                                      % uow.process_name)
                    continue

                process_entry = self.managed_entries[uow.process_name]
                assert isinstance(process_entry, ManagedProcessEntry)
                if not process_entry.is_on:
                    self.logger.debug('Process %r is inactive at the Synergy Scheduler. Skipping its unit_of_work.'
                                      % uow.process_name)
                    continue

                self._process_single_document(uow)

        except LookupError as e:
            self.logger.info('Normal behaviour. %r' % e)
        except Exception as e:
            self.logger.error('_mq_callback: %s' % str(e), exc_info=True)
        finally:
            self.consumer.acknowledge(message.delivery_tag)

    def _update_managed_entries(self, managed_entries):
        self.managed_entries = {me.process_name: me for me in managed_entries}

    def _process_single_document(self, uow):
        """ actually inspects UOW retrieved from the database"""
        repost = False
        if uow.is_invalid:
            repost = True

        elif uow.is_in_progress or uow.is_requested:
            last_activity = uow.started_at
            if last_activity is None:
                last_activity = uow.created_at

            if datetime.utcnow() - last_activity > timedelta(hours=REPOST_AFTER_HOURS):
                repost = True

        if repost:
            mq_request = SynergyMqTransmission(process_name=uow.process_name,
                                               unit_of_work_id=uow.db_id)

            if datetime.utcnow() - uow.created_at < timedelta(hours=LIFE_SUPPORT_HOURS):
                uow.state = unit_of_work.STATE_REQUESTED
                uow.number_of_retries += 1
                self.uow_dao.update(uow)

                publisher = self.publishers.get(uow.process_name)
                publisher.publish(mq_request.document)
                publisher.release()

                self.logger.info('UOW marked for re-processing: process %s; timeperiod %s; id %s; attempt %d'
                                 % (uow.process_name, uow.timeperiod, uow.db_id, uow.number_of_retries))
                self.performance_ticker.tracker.increment_success()
            else:
                uow.state = unit_of_work.STATE_CANCELED
                self.uow_dao.update(uow)

                publisher = self.publishers.get(QUEUE_UOW_REPORT)
                publisher.publish(mq_request.document)
                publisher.release()

                self.logger.info('UOW transferred to STATE_CANCELED: process %s; timeperiod %s; id %s; attempt %d'
                                 % (uow.process_name, uow.timeperiod, uow.db_id, uow.number_of_retries))


if __name__ == '__main__':
    from synergy.scheduler.scheduler_constants import PROCESS_GC

    source = GarbageCollectorWorker(PROCESS_GC)
    source.start()
