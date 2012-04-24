"""
Created on 2011-01-24
Module re-launches invalid units_of_work

@author: Bohdan Mushkevych
"""

import psutil
from threading import Lock
from pymongo import ASCENDING

from datetime import datetime, timedelta, time
from model.abstract_model import AbstractModel
from flopsy.flopsy import PublishersPool
from workers.abstract_worker import AbstractWorker
from scheduler.unit_of_work_entry import UnitOfWorkEntry
from scheduler.time_table import thread_safe
from system.collection_context import CollectionContext, COLLECTION_SINGLE_SESSION
from system.collection_context import COLLECTION_UNITS_OF_WORK
from system.repeat_timer import RepeatTimer
from system import time_helper
from scheduler import unit_of_work_helper
from settings import settings


LIFE_SUPPORT_HOURS = 48 # number of hours from UOW creation time to keep UOW re-posting to MQ
REPOST_AFTER_HOURS = 1  # number of hours, GC waits for the worker to pick up the UOW from MQ before re-posting

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
            query = { UnitOfWorkEntry.STATE : { '$in' : [UnitOfWorkEntry.STATE_IN_PROGRESS,
                                                               UnitOfWorkEntry.STATE_INVALID,
                                                               UnitOfWorkEntry.STATE_REQUESTED]}}
            cursor = self.collection.find(query).sort('_id', ASCENDING)
            
            if cursor.count() != 0:
                for document in cursor:
                    self._process_single_document(document)
        except Exception as e:
            self.logger.error('_mq_callback: %s' % str(e), exc_info = True)
        finally:
            self.consumer.acknowledge(message.delivery_tag)

    def _process_single_document(self, document):
        """ actually inspects UOW retrieved from the database"""
        repost = False
        unit_of_work = UnitOfWorkEntry(document)
        process_name = unit_of_work.get_process_name()

        if unit_of_work.get_state() == UnitOfWorkEntry.STATE_INVALID:
            repost = True
        elif unit_of_work.get_state() == UnitOfWorkEntry.STATE_IN_PROGRESS \
            or unit_of_work.get_state() == UnitOfWorkEntry.STATE_REQUESTED:

            last_activity = unit_of_work.get_started_at()
            if last_activity is None:
                last_activity = unit_of_work.get_created_at()

            if  datetime.utcnow() - last_activity > timedelta(hours=REPOST_AFTER_HOURS):
                repost = True
            
        if repost:
            creation_time = unit_of_work.get_created_at()
            if  datetime.utcnow() - creation_time < timedelta(hours=LIFE_SUPPORT_HOURS):
                unit_of_work.set_state(UnitOfWorkEntry.STATE_REQUESTED)
                unit_of_work.set_number_of_retries(unit_of_work.get_number_of_retries() + 1)
                unit_of_work_helper.update(self.logger, unit_of_work)
                self.publishers.get_publisher(process_name).publish(str(document['_id']))

                self.logger.info('UOW marked for re-processing: process %s; id %s; attempt %d' \
                                 % (process_name, str(document['_id']), unit_of_work.get_number_of_retries()))
                self.performance_ticker.increment()
            else:
                unit_of_work.set_state(UnitOfWorkEntry.STATE_CANCELED)
                unit_of_work_helper.update(self.logger, unit_of_work)
                self.logger.info('UOW transfered to STATE_CANCELED: process %s; id %s; attempt %d' \
                                 % (process_name, str(document['_id']), unit_of_work.get_number_of_retries()))


if __name__ == '__main__':
    from system.process_context import PROCESS_GC

    source = GarbageCollectorWorker(PROCESS_GC)
    source.start()
