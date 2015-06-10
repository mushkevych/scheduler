__author__ = 'Bohdan Mushkevych'

import gc
import json
import socket
from datetime import datetime

from synergy.conf import context
from synergy.db.model import unit_of_work
from synergy.db.model.synergy_mq_transmission import SynergyMqTransmission
from synergy.db.manager import ds_manager
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.conf import settings
from synergy.system.decimal_encoder import DecimalEncoder
from synergy.workers.abstract_mq_worker import AbstractMqWorker
from synergy.system.performance_tracker import UowAwareTracker


class AbstractMongoWorker(AbstractMqWorker):
    """ Abstract class is inherited by all workers of the illustration suite
    Module holds logic on handling unit_of_work and declaration of abstract methods """

    def __init__(self, process_name):
        super(AbstractMongoWorker, self).__init__(process_name)
        self.aggregated_objects = dict()
        self.source = context.process_context[self.process_name].source
        self.sink = context.process_context[self.process_name].sink
        self.uow_dao = UnitOfWorkDao(self.logger)
        self.ds = ds_manager.ds_factory(self.logger)

    def __del__(self):
        self._flush_aggregated_objects()
        super(AbstractMongoWorker, self).__del__()

    # **************** Abstract Methods ************************
    def _init_performance_ticker(self, logger):
        self.performance_ticker = UowAwareTracker(logger)
        self.performance_ticker.start()

    def _flush_aggregated_objects(self):
        """ method inserts aggregated objects into MongoDB
            @return number_of_aggregated_objects """
        if len(self.aggregated_objects) == 0:
            # nothing to do
            return 0

        number_of_aggregated_objects = len(self.aggregated_objects)
        self.logger.info('Aggregated %d documents. Performing flush.' % number_of_aggregated_objects)

        for key in self.aggregated_objects:
            document = self.aggregated_objects[key]
            mongo_pk = self._mongo_sink_key(*key)
            self.ds.update(self.sink, mongo_pk, document.document)

        self.logger.info('Flush successful.')

        del self.aggregated_objects
        self.aggregated_objects = dict()
        gc.collect()
        return number_of_aggregated_objects

    def _get_aggregated_object(self, composite_key):
        """ method talks with the map of instances of aggregated objects
        :param composite_key presents tuple, comprising of domain_name and timeperiod"""
        if composite_key not in self.aggregated_objects:
            self.aggregated_objects[composite_key] = self._init_sink_object(composite_key)
        return self.aggregated_objects[composite_key]

    def _init_sink_key(self, *args):
        """ abstract method to create composite key from source compounds like domain_name and timeperiod"""
        pass

    def _mongo_sink_key(self, *args):
        """ abstract method to create MongoDB primary key from source compounds like domain_name and timeperiod"""
        pass

    def _init_sink_object(self, composite_key):
        """ abstract method to instantiate new object that will be holding aggregated data """
        pass

    def _init_source_object(self, document):
        """ abstract method to initialise object with map from source collection """
        pass

    # ********************** thread-related methods ****************************
    def _process_not_empty_cursor(self, cursor):
        """ abstract method to process cursor with result set from DB
        method returns:
        shall_continue - True if outer loop shall continue
        new_start_id - mongo.ObjectId of the next start point"""
        pass

    def perform_post_processing(self, timeperiod):
        """ abstract method to perform post-processing (before flushing)"""
        pass

    def _mq_callback(self, message):
        """ try/except wrapper
        in case exception breaks the abstract method, this method:
        - catches the exception
        - logs the exception
        - marks unit of work as INVALID"""
        try:
            mq_request = SynergyMqTransmission.from_json(message.body)
            uow = self.uow_dao.get_one(mq_request.unit_of_work_id)
            if not uow.is_requested:
                # accept only UOW in STATE_REQUESTED
                self.logger.warn('Skipping unit_of_work: id %s; state %s;' % (str(message.body), uow.state),
                                 exc_info=False)
                self.consumer.acknowledge(message.delivery_tag)
                return
        except Exception:
            self.logger.error('Safety fuse. Can not identify unit_of_work %s' % str(message.body), exc_info=True)
            self.consumer.acknowledge(message.delivery_tag)
            return

        try:
            start_id_obj = uow.start_id
            end_id_obj = uow.end_id
            start_timeperiod = uow.start_timeperiod
            end_timeperiod = uow.end_timeperiod

            uow.state = unit_of_work.STATE_IN_PROGRESS
            uow.started_at = datetime.utcnow()
            self.uow_dao.update(uow)
            self.performance_ticker.start_uow(uow)

            bulk_threshold = settings.settings['bulk_threshold']
            iteration = 0
            while True:
                collection_name = context.process_context[self.process_name].source
                cursor = self.ds.cursor_for(collection_name,
                                            start_id_obj,
                                            end_id_obj,
                                            iteration,
                                            start_timeperiod,
                                            end_timeperiod,
                                            bulk_threshold)
                count = cursor.count(with_limit_and_skip=True)
                if count == 0 and iteration == 0:
                    msg = 'No entries in %s at range [%s : %s]' % (collection_name, uow.start_id, uow.end_id)
                    self.logger.warn(msg)
                    break
                else:
                    shall_continue, new_start_id = self._process_not_empty_cursor(cursor)
                    if shall_continue:
                        start_id_obj = new_start_id
                        iteration += 1
                    else:
                        break

            msg = 'Cursor exploited after %s iterations' % str(iteration)
            self.logger.info(msg)

            self.perform_post_processing(uow.timeperiod)
            number_of_aggregated_objects = self._flush_aggregated_objects()
            uow.number_of_aggregated_documents = number_of_aggregated_objects
            uow.number_of_processed_documents = self.performance_ticker.per_job
            uow.finished_at = datetime.utcnow()
            uow.state = unit_of_work.STATE_PROCESSED
            self.uow_dao.update(uow)
            self.performance_ticker.finish_uow()
        except Exception as e:
            fresh_uow = self.uow_dao.get_one(mq_request.unit_of_work_id)
            if fresh_uow.is_canceled:
                self.logger.warn('unit_of_work: id %s was likely marked by MX as SKIPPED. '
                                 'No unit_of_work update is performed.' % str(message.body),
                                 exc_info=False)
            else:
                self.logger.error('Safety fuse while processing unit_of_work %s in timeperiod %s : %r'
                                  % (message.body, uow.timeperiod, e), exc_info=True)
                uow.state = unit_of_work.STATE_INVALID
                self.uow_dao.update(uow)
                self.performance_ticker.cancel_uow()

            del self.aggregated_objects
            self.aggregated_objects = dict()
            gc.collect()

            self.logger.error('Safety fuse while processing unit_of_work %s in timeperiod %s : %r'
                              % (message.body, uow.timeperiod, e), exc_info=True)
        finally:
            self.consumer.acknowledge(message.delivery_tag)
            self.consumer.close()
