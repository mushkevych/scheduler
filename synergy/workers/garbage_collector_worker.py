__author__ = 'Bohdan Mushkevych'

from threading import Lock
from datetime import datetime, timedelta

from synergy.conf import settings
from synergy.mq.flopsy import PublishersPool
from synergy.system.decorator import thread_safe
from synergy.workers.abstract_mq_worker import AbstractMqWorker
from synergy.db.model import unit_of_work, scheduler_managed_entry
from synergy.db.model.worker_mq_request import WorkerMqRequest
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.model.scheduler_managed_entry import SchedulerManagedEntry
from synergy.db.dao.scheduler_managed_entry_dao import SchedulerManagedEntryDao


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
        self.managed_dao = SchedulerManagedEntryDao(self.logger)
        self.scheduler_configuration = dict()

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
            sc_list = self.managed_dao.get_all()
            self._update_scheduler_configuration(sc_list)

            since = settings.settings['synergy_start_timeperiod']
            uow_list = self.uow_dao.get_reprocessing_candidates(since)
            for uow in uow_list:
                if uow.process_name not in self.scheduler_configuration:
                    self.logger.debug('Process %r is not known to the Synergy Scheduler. Skipping its unit_of_work.'
                                      % uow.process_name)
                    continue

                process_config = self.scheduler_configuration[uow.process_name]
                assert isinstance(process_config, SchedulerManagedEntry)
                if process_config.state != scheduler_managed_entry.STATE_ON:
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

    def _update_scheduler_configuration(self, sc_list):
        self.scheduler_configuration = {sc.process_name: sc for sc in sc_list}

    def _process_single_document(self, uow):
        """ actually inspects UOW retrieved from the database"""
        repost = False
        if uow.state == unit_of_work.STATE_INVALID:
            repost = True

        elif uow.state in [unit_of_work.STATE_IN_PROGRESS, unit_of_work.STATE_REQUESTED]:
            last_activity = uow.started_at
            if last_activity is None:
                last_activity = uow.created_at

            if datetime.utcnow() - last_activity > timedelta(hours=REPOST_AFTER_HOURS):
                repost = True

        if repost:
            creation_time = uow.created_at
            if datetime.utcnow() - creation_time < timedelta(hours=LIFE_SUPPORT_HOURS):
                uow.state = unit_of_work.STATE_REQUESTED
                uow.number_of_retries += 1
                self.uow_dao.update(uow)

                mq_request = WorkerMqRequest()
                mq_request.process_name = uow.process_name
                mq_request.unit_of_work_id = uow.db_id

                publisher = self.publishers.get(uow.process_name)
                publisher.publish(mq_request.document)
                publisher.release()

                self.logger.info('UOW marked for re-processing: process %s; timeperiod %s; id %s; attempt %d'
                                 % (uow.process_name, uow.timeperiod, uow.db_id, uow.number_of_retries))
                self.performance_ticker.tracker.increment_success()
            else:
                uow.state = unit_of_work.STATE_CANCELED
                self.uow_dao.update(uow)
                self.logger.info('UOW transferred to STATE_CANCELED: process %s; timeperiod %s; id %s; attempt %d'
                                 % (uow.process_name, uow.timeperiod, uow.db_id, uow.number_of_retries))


if __name__ == '__main__':
    from synergy.scheduler.scheduler_constants import PROCESS_GC

    source = GarbageCollectorWorker(PROCESS_GC)
    source.start()
