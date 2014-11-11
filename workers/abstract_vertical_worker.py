__author__ = 'Bohdan Mushkevych'

from workers.abstract_mongo_worker import AbstractMongoWorker


class AbstractVerticalWorker(AbstractMongoWorker):
    """ Class contains common logic for aggregators that work on sequential flow from the DB """

    def __init__(self, process_name):
        super(AbstractVerticalWorker, self).__init__(process_name)

    # ********************** thread-related methods ****************************
    def _process_single_document(self, document):
        """ abstract method that actually processes the document from source collection"""
        pass

    def _process_not_empty_cursor(self, cursor):
        """ abstract method to process cursor with result set from DB"""
        shall_continue = False
        new_start_id = None

        for document in cursor:
            new_start_id = document['_id']
            self.performance_ticker.increment()
            self._process_single_document(document)
        if new_start_id is not None:
            shall_continue = True

        return shall_continue, new_start_id
