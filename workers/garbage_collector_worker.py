""" Module re-launches invalid units_of_work """

__author__ = 'Bohdan Mushkevych'

from threading import Lock

from datetime import datetime, timedelta
from mq.flopsy import PublishersPool
from system.decorator import thread_safe
from workers.abstract_worker import AbstractWorker
from db.model import unit_of_work
from db.dao.unit_of_work_dao import UnitOfWorkDao


LIFE_SUPPORT_HOURS = 48  # number of hours from UOW creation time to keep UOW re-posting to MQ
REPOST_AFTER_HOURS = 1   # number of hours, GC waits for the worker to pick up the UOW from MQ before re-posting


class GarbageCollectorWorker(AbstractWorker):
    """ instance receives an empty message from RabbitMQ and triggers re-start of failed tasks """

    def __init__(self, process_name):
        super(GarbageCollectorWorker, self).__init__(process_name)
        self.lock = Lock()
        self.publishers = PublishersPool(self.logger)
        self.uow_dao = UnitOfWorkDao(self.logger)

    def __del__(self):
        super(GarbageCollectorWorker, self).__del__()

    @thread_safe
    def _mq_callback(self, message):
        """ method looks for units of work in STATE_INVALID and re-runs them"""
        try:
            uow_list = self.uow_dao.get_reprocessing_candidates()
            for uow in uow_list:
                self._process_single_document(uow)
        except LookupError as e:
            self.logger.info('Normal behaviour. %r' % e)
        except Exception as e:
            self.logger.error('_mq_callback: %s' % str(e), exc_info=True)
        finally:
            self.consumer.acknowledge(message.delivery_tag)

    def _process_single_document(self, uow):
        """ actually inspects UOW retrieved from the database"""
        repost = False
        process_name = uow.process_name

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
                self.publishers.get_publisher(process_name).publish(str(uow.document['_id']))

                self.logger.info('UOW marked for re-processing: process %s; id %s; attempt %d'
                                 % (process_name, str(uow.document['_id']), uow.number_of_retries))
                self.performance_ticker.increment()
            else:
                uow.state = unit_of_work.STATE_CANCELED
                self.uow_dao.update(uow)
                self.logger.info('UOW transferred to STATE_CANCELED: process %s; id %s; attempt %d'
                                 % (process_name, str(uow.document['_id']), uow.number_of_retries))


if __name__ == '__main__':
    from system.process_context import PROCESS_GC

    source = GarbageCollectorWorker(PROCESS_GC)
    source.start()
