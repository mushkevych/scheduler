""" Module contains common logic for aggregators and workers that work with unit_of_work """

__author__ = 'Bohdan Mushkevych'

import gc
import json
import socket
from datetime import datetime

from bson.objectid import ObjectId
from pymongo import ASCENDING

from model import unit_of_work_helper, unit_of_work
from model import base_model
from settings import settings
from system.decimal_encoder import DecimalEncoder
from system.process_context import ProcessContext
from system.collection_context import CollectionContext
from workers.abstract_worker import AbstractWorker
from system.performance_ticker import AggregatorPerformanceTicker


class AbstractAwareWorker(AbstractWorker):
    """ Abstract class is inherited by all workers/aggregators
    that are aware of unit_of_work and capable of processing it"""

    def __init__(self, process_name):
        super(AbstractAwareWorker, self).__init__(process_name)
        self.aggregated_objects = dict()

    def __del__(self):
        self._flush_aggregated_objects()
        super(AbstractAwareWorker, self).__del__()

    # **************** Abstract Methods ************************
    def _init_performance_ticker(self, logger):
        self.performance_ticker = AggregatorPerformanceTicker(logger)
        self.performance_ticker.start()

    def _get_tunnel_port(self):
        """ abstract method to retrieve Python-HBase tunnel port"""
        pass

    def _get_source_collection(self):
        """collection with data for processing"""
        return CollectionContext.get_collection(self.logger, ProcessContext.get_source_collection(self.process_name))

    def _get_target_collection(self):
        """collection to store aggregated documents"""
        return CollectionContext.get_collection(self.logger, ProcessContext.get_target_collection(self.process_name))

    def _flush_aggregated_objects(self):
        """ method inserts aggregated objects to HBaseTunnel
            @return number_of_aggregated_objects """
        if len(self.aggregated_objects) == 0:
            # nothing to do
            return 0

        total_transferred_bytes = 0
        number_of_aggregated_objects = len(self.aggregated_objects)
        self.logger.info('Aggregated %d documents. Performing flush.' % number_of_aggregated_objects)
        tunnel_address = (settings['tunnel_host'], self._get_tunnel_port())

        for key in self.aggregated_objects:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(tunnel_address)
            document = self.aggregated_objects[key]
            tunnel_obj = json.dumps(document.data, cls=DecimalEncoder)
            transferred_bytes = client_socket.send(tunnel_obj)
            if transferred_bytes == 0:
                raise RuntimeError("Transferred 0 bytes. Socket connection broken")
            total_transferred_bytes += transferred_bytes
            client_socket.shutdown(socket.SHUT_WR)
            client_socket.close()

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(tunnel_address)
        transferred_bytes = client_socket.send('FLUSH')
        if transferred_bytes == 0:
            raise RuntimeError("Transferred 0 bytes. Socket connection broken")
        client_socket.close()
        self.logger.info('Flush successful. Transmitted %r bytes' % total_transferred_bytes)

        del self.aggregated_objects
        self.aggregated_objects = dict()
        gc.collect()
        return number_of_aggregated_objects

    def _get_aggregated_object(self, composite_key):
        """ method talks with the map of instances of aggregated objects
        @param composite_key presents tuple, comprising of domain_name and timestamp"""
        if composite_key not in self.aggregated_objects:
            self.aggregated_objects[composite_key] = self._init_target_object(composite_key)
        return self.aggregated_objects[composite_key]

    def _init_target_key(self, *args):
        """ abstract method to create composite key from source compounds like domain_name and timestamp"""
        pass

    def _init_target_object(self, composite_key):
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

    def perform_post_processing(self, timestamp):
        """ abstract method to perform post-processing (before flushing)"""
        pass

    def _mq_callback(self, message):
        """ try/except wrapper
        in case exception breaks the abstract method, this method:
        - catches the exception
        - logs the exception
        - marks unit of work as INVALID"""
        uow = None
        try:
            # @param object_id: ObjectId of the unit_of_work from mq
            object_id = ObjectId(message.body)
            uow = unit_of_work_helper.retrieve_by_id(self.logger, object_id)
            if uow.state == unit_of_work.STATE_CANCELED \
                or uow.state == unit_of_work.STATE_PROCESSED:
                # garbage collector might have reposted this UOW
                self.logger.warning('Skipping unit_of_work: id %s; state %s;' % (str(message.body), uow.state),
                                    exc_info=False)
                self.consumer.acknowledge(message.delivery_tag)
                return
        except Exception:
            self.logger.error('Safety fuse. Can not identify unit_of_work %s' % str(message.body), exc_info=True)
            self.consumer.acknowledge(message.delivery_tag)
            return

        try:
            start_id_obj = ObjectId(uow.start_id)
            end_id_obj = ObjectId(uow.end_id)
            start_timeperiod = uow.start_timeperiod
            end_timeperiod = uow.end_timeperiod

            uow.state = unit_of_work.STATE_IN_PROGRESS
            uow.started_at = datetime.utcnow()
            unit_of_work_helper.update(self.logger, uow)
            self.performance_ticker.start_uow(uow)

            bulk_threshold = settings['bulk_threshold']
            iteration = 0
            while True:
                source_collection = self._get_source_collection()
                if iteration == 0:
                    queue = {'_id': {'$gte': start_id_obj, '$lte': end_id_obj}}
                else:
                    queue = {'_id': {'$gt': start_id_obj, '$lte': end_id_obj}}

                if start_timeperiod is not None and end_timeperiod is not None:
                    # remove all accident objects that may be in [start_id_obj : end_id_obj] range
                    queue[base_model.TIMEPERIOD] = {'$gte': start_timeperiod, '$lt': end_timeperiod}

                cursor = source_collection.find(queue).sort('_id', ASCENDING).limit(bulk_threshold)
                count = cursor.count(with_limit_and_skip=True)
                if count == 0 and iteration == 0:
                    msg = 'No entries in %s at range [%s : %s]' % (
                    str(source_collection.name), uow.start_id, uow.end_id)
                    self.logger.warning(msg)
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
            uow.number_of_processed_documents = self.performance_ticker.posts_per_job
            uow.finished_at = datetime.utcnow()
            uow.state = unit_of_work.STATE_PROCESSED
            unit_of_work_helper.update(self.logger, uow)
            self.performance_ticker.finish_uow()
        except Exception as e:
            uow.state = unit_of_work.STATE_INVALID
            unit_of_work_helper.update(self.logger, uow)
            self.performance_ticker.cancel_uow()

            del self.aggregated_objects
            self.aggregated_objects = dict()
            gc.collect()

            self.logger.error('Safety fuse while processing unit_of_work %s in timeperiod %s : %r'
                              % (message.body, uow.timeperiod, e), exc_info=True)
        finally:
            self.consumer.acknowledge(message.delivery_tag)
            self.consumer.close()
