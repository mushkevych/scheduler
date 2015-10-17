__author__ = 'Bohdan Mushkevych'

from threading import RLock

from bson.objectid import ObjectId
from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError as MongoDuplicateKeyError

from synergy.system import time_helper
from synergy.system.time_qualifier import *
from synergy.system.decorator import thread_safe
from synergy.scheduler.scheduler_constants import COLLECTION_UNIT_OF_WORK
from synergy.conf import context
from synergy.db.error import DuplicateKeyError
from synergy.db.model import unit_of_work
from synergy.db.model.unit_of_work import UnitOfWork
from synergy.db.manager import ds_manager

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
        if not isinstance(key, ObjectId):
            # cast key to ObjectId
            key = ObjectId(key)

        query = {'_id': key}
        collection = self.ds.connection(COLLECTION_UNIT_OF_WORK)

        document = collection.find_one(query)
        if document is None:
            msg = 'UOW with ID={0} was not found'.format(key)
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
    def get_by_params(self, process_name, timeperiod, start_obj_id, end_obj_id):
        """ method finds unit_of_work record and returns it to the caller"""
        query = {unit_of_work.PROCESS_NAME: process_name,
                 unit_of_work.TIMEPERIOD: timeperiod,
                 unit_of_work.START_OBJ_ID: start_obj_id,
                 unit_of_work.END_OBJ_ID: end_obj_id}
        collection = self.ds.connection(COLLECTION_UNIT_OF_WORK)

        document = collection.find_one(query)
        if document is None:
            raise LookupError('UOW satisfying query {0} was not found'.format(query))
        return UnitOfWork.from_json(document)

    @thread_safe
    def update(self, instance):
        """ method finds unit_of_work record and change its status"""
        assert isinstance(instance, UnitOfWork)
        collection = self.ds.connection(COLLECTION_UNIT_OF_WORK)
        document = instance.document
        if instance.db_id:
            document['_id'] = ObjectId(instance.db_id)
        instance.db_id = collection.save(document)
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
                                    instance.start_timeperiod,
                                    instance.start_id,
                                    instance.end_id,
                                    e)
            raise exc

    @thread_safe
    def remove(self, uow_id):
        assert isinstance(uow_id, (str, ObjectId))
        collection = self.ds.connection(COLLECTION_UNIT_OF_WORK)
        return collection.delete_one(filter={'_id': uow_id}).raw_result

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
                self.logger.error('Unable to recover from DuplicateKeyError error due to {0}'.format(e), exc_info=True)
        else:
            msg = 'Unable to recover from DuplicateKeyError due to unspecified UOW primary key'
            self.logger.error(msg)
