__author__ = 'Bohdan Mushkevych'

from threading import RLock

from bson.objectid import ObjectId
from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError as MongoDuplicateKeyError

from synergy.conf import context
from synergy.db.error import DuplicateKeyError
from synergy.db.manager import ds_manager
from synergy.db.model import unit_of_work
from synergy.db.model.unit_of_work import UnitOfWork
from synergy.scheduler.scheduler_constants import COLLECTION_UNIT_OF_WORK
from synergy.system import time_helper
from synergy.system.decorator import thread_safe
from synergy.system.time_qualifier import *

QUERY_GET_FREERUN_SINCE = lambda timeperiod, include_running, include_processed, include_noop, include_failed: {
    unit_of_work.TIMEPERIOD: {'$gte': timeperiod},
    unit_of_work.UNIT_OF_WORK_TYPE: unit_of_work.TYPE_FREERUN,
    unit_of_work.STATE: {'$in': [unit_of_work.STATE_PROCESSED if include_processed else None,
                                 unit_of_work.STATE_IN_PROGRESS if include_running else None,
                                 unit_of_work.STATE_REQUESTED if include_running else None,
                                 unit_of_work.STATE_CANCELED if include_failed else None,
                                 unit_of_work.STATE_INVALID if include_failed else None,
                                 unit_of_work.STATE_NOOP if include_noop else None]}
}

QUERY_GET_FLOW_FREERUNS = lambda schedulable_stem, timeperiod: {
    unit_of_work.TIMEPERIOD: timeperiod,
    unit_of_work.UNIT_OF_WORK_TYPE: unit_of_work.TYPE_FREERUN,
    unit_of_work.PROCESS_NAME: {'$regex': schedulable_stem + '*', '$options': 'i'}
}


class UnitOfWorkDao(object):
    """ Thread-safe Data Access Object from units_of_work table/collection """

    def __init__(self, logger):
        super(UnitOfWorkDao, self).__init__()
        self.logger = logger
        self.lock = RLock()
        self.ds = ds_manager.ds_factory(logger)

    @thread_safe
    def get_one(self, key):
        """ method finds unit_of_work record and returns it to the caller"""
        query = {'_id': ObjectId(key)}
        collection = self.ds.connection(COLLECTION_UNIT_OF_WORK)

        document = collection.find_one(query)
        if document is None:
            msg = f'UOW with ID={key} was not found'
            self.logger.warning(msg)
            raise LookupError(msg)
        return UnitOfWork.from_json(document)

    @thread_safe
    def get_reprocessing_candidates(self, since=None):
        """ method queries Unit Of Work whose <start_timeperiod> is younger than <since>
        and who could be candidates for re-processing """
        collection = self.ds.connection(COLLECTION_UNIT_OF_WORK)

        query = {unit_of_work.STATE: {'$in': [unit_of_work.STATE_IN_PROGRESS,
                                              unit_of_work.STATE_INVALID,
                                              unit_of_work.STATE_REQUESTED]},
                 unit_of_work.UNIT_OF_WORK_TYPE: unit_of_work.TYPE_MANAGED}

        if since is None:
            cursor = collection.find(query).sort('_id', ASCENDING)
            candidates = [UnitOfWork.from_json(document) for document in cursor]
        else:
            candidates = []
            yearly_timeperiod = time_helper.cast_to_time_qualifier(QUALIFIER_YEARLY, since)
            query[unit_of_work.START_TIMEPERIOD] = {'$gte': yearly_timeperiod}

            cursor = collection.find(query).sort('_id', ASCENDING)
            for document in cursor:
                uow = UnitOfWork.from_json(document)
                if uow.process_name not in context.process_context:
                    # this is a decommissioned process
                    continue

                time_qualifier = context.process_context[uow.process_name].time_qualifier
                if time_qualifier == QUALIFIER_REAL_TIME:
                    time_qualifier = QUALIFIER_HOURLY
                process_specific_since = time_helper.cast_to_time_qualifier(time_qualifier, since)

                if process_specific_since <= uow.start_timeperiod:
                    candidates.append(uow)

        if len(candidates) == 0:
            raise LookupError('MongoDB has no UOW reprocessing candidates')
        return candidates

    @thread_safe
    def get_by_params(self, process_name, timeperiod, start_id, end_id):
        """ method finds unit_of_work record and returns it to the caller"""
        query = {unit_of_work.PROCESS_NAME: process_name,
                 unit_of_work.TIMEPERIOD: timeperiod,
                 unit_of_work.START_ID: start_id,
                 unit_of_work.END_ID: end_id}
        collection = self.ds.connection(COLLECTION_UNIT_OF_WORK)

        document = collection.find_one(query)
        if document is None:
            raise LookupError(f'UOW satisfying query {query} was not found')
        return UnitOfWork.from_json(document)

    @thread_safe
    def update(self, instance):
        """ method finds unit_of_work record and change its status"""
        assert isinstance(instance, UnitOfWork)
        if instance.db_id:
            query = {'_id': ObjectId(instance.db_id)}
        else:
            query = {unit_of_work.PROCESS_NAME: instance.process_name,
                     unit_of_work.TIMEPERIOD: instance.timeperiod,
                     unit_of_work.START_ID: instance.start_id,
                     unit_of_work.END_ID: instance.end_id}
        self.ds.update(COLLECTION_UNIT_OF_WORK, query, instance)
        return instance.db_id

    @thread_safe
    def insert(self, instance):
        """ inserts a unit of work into MongoDB.
        :raises DuplicateKeyError: if such record already exist """
        assert isinstance(instance, UnitOfWork)
        collection = self.ds.connection(COLLECTION_UNIT_OF_WORK)
        try:
            return collection.insert_one(instance.document).inserted_id
        except MongoDuplicateKeyError as e:
            exc = DuplicateKeyError(instance.process_name,
                                    instance.timeperiod,
                                    instance.start_id,
                                    instance.end_id,
                                    e)
            raise exc

    @thread_safe
    def remove(self, uow_id):
        collection = self.ds.connection(COLLECTION_UNIT_OF_WORK)
        collection.delete_one({'_id': ObjectId(uow_id)})

    @thread_safe
    def run_query(self, query):
        """ method runs the query and returns a list of filtered UnitOfWork records """
        cursor = self.ds.filter(COLLECTION_UNIT_OF_WORK, query)
        return [UnitOfWork.from_json(document) for document in cursor]

    def recover_from_duplicatekeyerror(self, e):
        """ method tries to recover from DuplicateKeyError """
        if isinstance(e, DuplicateKeyError):
            try:
                return self.get_by_params(e.process_name, e.timeperiod, e.start_id, e.end_id)
            except LookupError as e:
                self.logger.error(f'Unable to recover from DuplicateKeyError error due to {e}', exc_info=True)
        else:
            msg = 'Unable to recover from DuplicateKeyError due to unspecified UOW primary key'
            self.logger.error(msg)
