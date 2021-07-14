__author__ = 'Bohdan Mushkevych'

import gc

from synergy.conf import context
from synergy.db.model import unit_of_work
from synergy.db.manager import get_data_source
from synergy.workers.abstract_uow_aware_worker import AbstractUowAwareWorker


class AbstractMongoWorker(AbstractUowAwareWorker):
    """ Abstract class is inherited by all workers of the illustration suite
    Module holds logic on handling unit_of_work and declaration of abstract methods """

    def __init__(self, process_name):
        super(AbstractMongoWorker, self).__init__(process_name, perform_db_logging=True)
        self.aggregated_objects = dict()
        self.source = context.process_context[self.process_name].source
        self.sink = context.process_context[self.process_name].sink
        self.ds = get_data_source(self.logger)

    def __del__(self):
        self._flush_aggregated_objects()
        super(AbstractMongoWorker, self).__del__()

    # **************** Abstract Methods ************************
    def _flush_aggregated_objects(self):
        """ method inserts aggregated objects into MongoDB
            :return number_of_aggregated_objects """
        if len(self.aggregated_objects) == 0:
            # nothing to do
            return 0

        number_of_aggregated_objects = len(self.aggregated_objects)
        self.logger.info(f'Aggregated {number_of_aggregated_objects} documents. Performing flush.')

        for key in self.aggregated_objects:
            document = self.aggregated_objects[key]
            mongo_pk = self._mongo_sink_key(*key)
            self.ds.update(self.sink, mongo_pk, document)

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
    def _process_single_document(self, document):
        """ abstract method that actually processes the document from source collection"""
        pass

    def _cursor_exploited(self):
        """ abstract method notifying users that cursor was exploited """
        pass

    def _run_custom_data_engine(self, start_id_obj, end_id_obj, start_timeperiod, end_timeperiod):
        """ fine-tuned data engine. MongoDB legacy """
        collection_name = context.process_context[self.process_name].source
        iteration = 0
        while True:
            cursor = self.ds.cursor_fine(collection_name,
                                         start_id_obj,
                                         end_id_obj,
                                         iteration,
                                         start_timeperiod,
                                         end_timeperiod)

            if iteration == 0 and cursor.count(with_limit_and_skip=True) == 0:
                msg = f'No entries in {collection_name} at range [{start_id_obj} : {end_id_obj}]'
                self.logger.warning(msg)
                break

            start_id_obj = None
            for document in cursor:
                start_id_obj = document['_id']
                self._process_single_document(document)
                self.performance_tracker.increment_success()
            if start_id_obj is None:
                break
            iteration += 1

        self._cursor_exploited()
        msg = f'Cursor exploited after {iteration} iterations'
        self.logger.info(msg)

    def _run_data_engine(self, start_timeperiod, end_timeperiod):
        """ regular data engine """
        collection_name = context.process_context[self.process_name].source
        cursor = self.ds.cursor_batch(collection_name,
                                      start_timeperiod,
                                      end_timeperiod)
        for document in cursor:
            self._process_single_document(document)
            self.performance_tracker.increment_success()

        self._cursor_exploited()
        msg = f'Cursor exploited after fetching {self.performance_tracker.success_per_job} documents'
        self.logger.info(msg)

    def _process_uow(self, uow):
        if not uow.start_id or not uow.end_id:
            self._run_data_engine(uow.start_timeperiod, uow.end_timeperiod)
        else:
            self._run_custom_data_engine(uow.start_id, uow.end_id, uow.start_timeperiod, uow.end_timeperiod)
        self._flush_aggregated_objects()
        return self.performance_tracker.success_per_job, unit_of_work.STATE_PROCESSED
