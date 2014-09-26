""" Module contains common logic for all workers that work with chunks of data from DB """
from synergy.db.model import base_model

__author__ = 'Bohdan Mushkevych'

from synergy.workers.abstract_aware_worker import AbstractAwareWorker


class AbstractHorizontalWorker(AbstractAwareWorker):
    """
    this class has different _engine_ that in contrary to sequential reading, reads bulks from DB  
    """

    def __init__(self, process_name):
        super(AbstractHorizontalWorker, self).__init__(process_name)

    def _process_bulk_array(self, array_of_documents, timeperiod):
        """ abstract method to parse the bulk of documents """
        pass

    def _process_not_empty_cursor(self, cursor):
        """ abstract method to process cursor with result set from DB"""
        shall_continue = False
        new_start_id = None

        try:
            bulk_array = []
            for document in cursor:
                new_start_id = document['_id']
                bulk_array.append(document)
                self.performance_ticker.increment()
            if len(bulk_array) > 0:
                # Mongo funny behaviour - cursor may be empty, with cursor.count != 0
                self._process_bulk_array(bulk_array, bulk_array[0][base_model.TIMEPERIOD])
                shall_continue = True
            del bulk_array
        except LookupError as e:
            self.logger.error('Some data is missing. Proceeding to next bulk read : %r' % e)

        return shall_continue, new_start_id