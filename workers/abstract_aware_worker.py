"""
Created on 2011-02-01

Module contains common logic for aggregators and workers that work with unit_of_work 

@author: Bohdan Mushkevych
"""
import gc
import json
import socket
from bson.objectid import ObjectId
from datetime import datetime
from pymongo import ASCENDING
from model import unit_of_work_helper
from model.abstract_model import AbstractModel

from settings import settings
from model.unit_of_work_entry import UnitOfWorkEntry
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

        total_transfered_bytes = 0
        number_of_aggregated_objects = len(self.aggregated_objects)
        self.logger.info('Aggregated %d documents. Performing flush.' % number_of_aggregated_objects)
        tunnel_address = (settings['tunnel_host'], self._get_tunnel_port())

        for key in self.aggregated_objects:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(tunnel_address)
            document = self.aggregated_objects[key]
            tunnel_obj = json.dumps(document.data, cls=DecimalEncoder)
            transfered_bytes = client_socket.send(tunnel_obj)
            if transfered_bytes == 0:
                raise RuntimeError("Transferred 0 bytes. Socket connection broken")
            total_transfered_bytes += transfered_bytes
            client_socket.shutdown(socket.SHUT_WR)
            client_socket.close()

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(tunnel_address)
        transfered_bytes = client_socket.send('FLUSH')
        if transfered_bytes == 0:
            raise RuntimeError("Transferred 0 bytes. Socket connection broken")
        client_socket.close()
        self.logger.info('Flush successful. Transmitted %r bytes' % total_transfered_bytes)

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
        unit_of_work = None
        try:
            # @param object_id: ObjectId of the unit_of_work from mq
            object_id = ObjectId(message.body)
            unit_of_work = unit_of_work_helper.retrieve_by_id(self.logger, object_id)
            if unit_of_work.get_state() == UnitOfWorkEntry.STATE_CANCELED \
                or unit_of_work.get_state() == UnitOfWorkEntry.STATE_PROCESSED:
                # garbage collector might have reposted this UOW
                self.logger.warning('Skipping unit_of_work: id %s; state %s;' \
                                    % (str(message.body), unit_of_work.get_state()), exc_info=False)
                self.consumer.acknowledge(message.delivery_tag)
                return
        except Exception:
            self.logger.error('Safety fuse. Can not identify unit_of_work %s' % str(message.body), exc_info=True)
            self.consumer.acknowledge(message.delivery_tag)
            return

        try:
            start_id_obj = ObjectId(unit_of_work.get_start_id())
            end_id_obj = ObjectId(unit_of_work.get_end_id())
            start_timestamp = unit_of_work.get_start_timestamp()
            end_timestamp = unit_of_work.get_end_timestamp()

            unit_of_work.set_state(UnitOfWorkEntry.STATE_IN_PROGRESS)
            unit_of_work.set_started_at(datetime.utcnow())
            unit_of_work_helper.update(self.logger, unit_of_work)
            self.performance_ticker.start_uow(unit_of_work)

            bulk_threshold = settings['bulk_threshold']
            iteration = 0
            while True:
                source_collection = self._get_source_collection()
                if iteration == 0:
                    queue = { '_id' : { '$gte' : start_id_obj, '$lte' : end_id_obj } }
                else:
                    queue = { '_id' : { '$gt' : start_id_obj, '$lte' : end_id_obj } }

                if start_timestamp is not None and end_timestamp is not None:
                    # remove all accident objects that may be in [start_id_obj : end_id_obj] range
                    queue[AbstractModel.TIMESTAMP] = { '$gte' : start_timestamp, '$lt' : end_timestamp }

                cursor = source_collection.find(queue).sort('_id', ASCENDING).limit(bulk_threshold)
                count = cursor.count(with_limit_and_skip=True)
                if count == 0 and iteration == 0:
                    msg = 'No entries in %s at range [%s : %s]'\
                            % (str(source_collection.name), unit_of_work.get_start_id(), unit_of_work.get_end_id())
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

            self.perform_post_processing(unit_of_work.get_timestamp())
            number_of_aggregated_objects = self._flush_aggregated_objects()
            unit_of_work.set_number_of_aggregated_documents(number_of_aggregated_objects)
            unit_of_work.set_number_of_processed_documents(self.performance_ticker.posts_per_job)
            unit_of_work.set_finished_at(datetime.utcnow())
            unit_of_work.set_state(UnitOfWorkEntry.STATE_PROCESSED)
            unit_of_work_helper.update(self.logger, unit_of_work)
            self.performance_ticker.finish_uow()
        except Exception as e:
            unit_of_work.set_state(UnitOfWorkEntry.STATE_INVALID)
            unit_of_work_helper.update(self.logger, unit_of_work)
            self.performance_ticker.cancel_uow()

            del self.aggregated_objects
            self.aggregated_objects = dict()
            gc.collect()

            self.logger.error('Safety fuse while processing unit_of_work %s in timeperiod %s : %r'\
                              % (message.body, unit_of_work.get_timestamp(), e), exc_info=True)
        finally:
            self.consumer.acknowledge(message.delivery_tag)
            self.consumer.close()
