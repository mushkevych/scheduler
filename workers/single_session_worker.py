""" Module is responsible for reading MQ queue and updating/inserting data records to the MongoDB """

__author__ = 'Bohdan Mushkevych'

import time
from pymongo.errors import AutoReconnect
from model.single_session import SingleSessionStatistics
from model.raw_data import *
from system.performance_ticker import SessionPerformanceTicker
from workers.abstract_worker import AbstractWorker
from system.collection_context import CollectionContext, COLLECTION_SINGLE_SESSION
from system import time_helper


class SingleSessionWorker(AbstractWorker):
    """
    this class reads stream of messages from RabbitMQ and dump them to the MongoDB
    """

    # every 15 minutes worker will perform <safe=True> save to Mongo DB
    # this allows to catch MongoDB connection expiration
    SAFE_SAVE_INTERVAL = 900

    def __init__(self, process_name):
        super(SingleSessionWorker, self).__init__(process_name)

    # ********************** abstract methods ****************************
    def _init_performance_ticker(self, logger):
        self.performance_ticker = SessionPerformanceTicker(logger)
        self.performance_ticker.start()
        self._last_safe_save_time = time.time()

    def _mq_callback(self, message):
        """ wraps call of abstract method with try/except 
        in case exception breaks the abstract method, this method:
        - catches the exception
        - logs the exception
        - marks unit of work as INVALID"""
        try:
            single_session_collection = CollectionContext.get_collection(self.logger, COLLECTION_SINGLE_SESSION)
            raw_data = RawData(message.body)
            query = {DOMAIN_NAME: raw_data.key[0],
                     FAMILY_USER_PROFILE + '.' + SESSION_ID: raw_data.session_id}
            document = single_session_collection.find_one(query)

            if document is None:
                # insert the record
                session = SingleSessionStatistics()

                # input data constraints - both session_id and user_id must be present in MQ message
                session.key = (raw_data.key[0], time_helper.raw_to_session(raw_data.key[1]))
                session.session_id = raw_data.session_id
                session.ip = raw_data.ip
                session.total_duration = 0

                session = self.update_session_body(raw_data, session)
                self.add_entry(session, 0, raw_data)
                self.performance_ticker.increment_insert()
            else:
                # update the click_xxx info
                session = SingleSessionStatistics(document)

                session = self.update_session_body(raw_data, session)
                duration = raw_data.key[1] - time_helper.session_to_epoch(session.key[1])
                session.total_duration = duration

                index = session.number_of_entries
                self.add_entry(session, index, raw_data)
                self.performance_ticker.increment_update()

            if time.time() - self._last_safe_save_time < self.SAFE_SAVE_INTERVAL:
                isSafe = False
            else:
                isSafe = True
                self._last_safe_save_time = time.time()

            single_session_collection.save(session.document, safe=isSafe)
            self.consumer.acknowledge(message.delivery_tag)
        except AutoReconnect as e:
            self.logger.error('MongoDB connection error: %r\nRe-queueing message & exiting the worker' % e)
            self.consumer.reject(message.delivery_tag)
            raise e
        except (KeyError, IndexError) as e:
            self.logger.error('Error is considered Unrecoverable: %r\nCancelled message: %r' % (e, message.body))
            self.consumer.cancel(message.delivery_tag)
        except Exception as e:
            self.logger.error('Error is considered Recoverable: %r\nRe-queueing message: %r' % (e, message.body))
            self.consumer.reject(message.delivery_tag)

    def update_session_body(self, raw_data, session):
        if raw_data.browser is not None:
            session.browser = raw_data.browser
        if raw_data.screen_res[0] is not None and raw_data.screen_res[1] is not None:
            session.screen_res = (raw_data.screen_res[0], raw_data.screen_res[1])
        if raw_data.os is not None:
            session.os = raw_data.os
        if raw_data.language is not None:
            session.language = raw_data.language
        if raw_data.country is not None:
            session.country = raw_data.country

        if session.number_of_pageviews is None:
            session.number_of_pageviews = 0
        if raw_data.is_page_view:
            session.number_of_pageviews += 1

        return session

    def add_entry(self, session, index, raw_data):
        session.number_of_entries = index + 1
        session.set_entry_timestamp(index, time_helper.raw_to_session(raw_data.key[1]))


if __name__ == '__main__':
    from system.process_context import PROCESS_SESSION_WORKER_00

    source = SingleSessionWorker(PROCESS_SESSION_WORKER_00)
    source.start()
