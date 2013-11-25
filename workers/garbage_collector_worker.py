""" Module re-launches invalid units_of_work """
from db.model import unit_of_work, unit_of_work_dao

__author__ = 'Bohdan Mushkevych'

from threading import Lock
from pymongo import ASCENDING

from datetime import datetime, timedelta
from flopsy.flopsy import PublishersPool
from system.decorator import thread_safe
from workers.abstract_worker import AbstractWorker
from db.model.unit_of_work import UnitOfWork
from system.collection_context import CollectionContext
from system.collection_context import COLLECTION_UNITS_OF_WORK


LIFE_SUPPORT_HOURS = 48  # number of hours from UOW creation time to keep UOW re-posting to MQ
REPOST_AFTER_HOURS = 1   # number of hours, GC waits for the worker to pick up the UOW from MQ before re-posting


class GarbageCollectorWorker(AbstractWorker):
    """
    this class reads stream of messages from RabbitMQ and dump them to the MongoDB
    """

    def __init__(self, process_name):
        super(GarbageCollectorWorker, self).__init__(process_name)
        self.publishers = PublishersPool(self.logger)
        self.collection = CollectionContext.get_collection(self.logger, COLLECTION_UNITS_OF_WORK)
        self.lock = Lock()

    def __del__(self):
        super(GarbageCollectorWorker, self).__del__()

    @thread_safe
    def _mq_callback(self, message):
        """ method looks for units of work in STATE_INVALID and re-runs them"""
        try:
            query = {unit_of_work.STATE: {'$in': [unit_of_work.STATE_IN_PROGRESS,
                                                  unit_of_work.STATE_INVALID,
                                                  unit_of_work.STATE_REQUESTED]}}
            cursor = self.collection.find(query).sort('_id', ASCENDING)

            if cursor.count() != 0:
                for document in cursor:
                    self._process_single_document(document)
        except Exception as e:
            self.logger.error('_mq_callback: %s' % str(e), exc_info=True)
        finally:
            self.consumer.acknowledge(message.delivery_tag)

    def _process_single_document(self, document):
        """ actually inspects UOW retrieved from the database"""
        repost = False
        uow = UnitOfWork(document)
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
                unit_of_work_dao.update(self.logger, uow)
                self.publishers.get_publisher(process_name).publish(str(document['_id']))

                self.logger.info('UOW marked for re-processing: process %s; id %s; attempt %d'
                                 % (process_name, str(document['_id']), uow.number_of_retries))
                self.performance_ticker.increment()
            else:
                uow.state = unit_of_work.STATE_CANCELED
                unit_of_work_dao.update(self.logger, uow)
                self.logger.info('UOW transferred to STATE_CANCELED: process %s; id %s; attempt %d'
                                 % (process_name, str(document['_id']), uow.number_of_retries))


if __name__ == '__main__':
    from system.process_context import PROCESS_GC

    source = GarbageCollectorWorker(PROCESS_GC)
    source.start()
