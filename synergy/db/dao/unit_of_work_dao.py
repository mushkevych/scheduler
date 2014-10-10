__author__ = 'Bohdan Mushkevych'

from threading import RLock

from bson.objectid import ObjectId
from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError as MongoDuplicateKeyError

from synergy.system import time_helper
from synergy.system.time_qualifier import *
from synergy.system.decorator import thread_safe
from synergy.scheduler.scheduler_constants import COLLECTION_UNIT_OF_WORK, TYPE_MANAGED
from synergy.conf.process_context import ProcessContext
from synergy.db.error import DuplicateKeyError
from synergy.db.model import unit_of_work
from synergy.db.model.unit_of_work import UnitOfWork
from synergy.db.manager import ds_manager


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
            msg = 'Unit_of_work with ID=%s was not found' % str(key)
            self.logger.warning(msg)
            raise LookupError(msg)
        return UnitOfWork(document)

    @thread_safe
    def get_reprocessing_candidates(self, since=None):
        """ method queries Unit Of Work whose <start_timeperiod> is younger than <since>
        and who could be candidates for re-processing """
        collection = self.ds.connection(COLLECTION_UNIT_OF_WORK)

        query = {unit_of_work.STATE: {'$in': [unit_of_work.STATE_IN_PROGRESS,
                                              unit_of_work.STATE_INVALID,
                                              unit_of_work.STATE_REQUESTED]},
                 unit_of_work.UNIT_OF_WORK_TYPE: TYPE_MANAGED}

        if since is None:
            cursor = collection.find(query).sort('_id', ASCENDING)
            candidates = [UnitOfWork(document) for document in cursor]
        else:
            candidates = []
            yearly_timeperiod = time_helper.cast_to_time_qualifier(QUALIFIER_YEARLY, since)
            query[unit_of_work.START_TIMEPERIOD] = {'$gte': yearly_timeperiod}

            cursor = collection.find(query).sort('_id', ASCENDING)
            for document in cursor:
                uow = UnitOfWork(document)
                if uow.process_name not in ProcessContext.CONTEXT:
                    # this is a decommissioned process
                    continue

                time_qualifier = ProcessContext.get_time_qualifier(uow.process_name)
                if time_qualifier == QUALIFIER_REAL_TIME:
                    time_qualifier = QUALIFIER_HOURLY
                process_specific_since = time_helper.cast_to_time_qualifier(time_qualifier, since)

                if process_specific_since <= uow.start_timeperiod:
                    candidates.append(uow)

        if len(candidates) == 0:
            raise LookupError('MongoDB has no reprocessing candidates units of work')
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
            raise LookupError('Unit_of_work satisfying query %r was not found' % query)
        return UnitOfWork(document)

    @thread_safe
    def update(self, instance):
        """ method finds unit_of_work record and change its status"""
        assert isinstance(instance, UnitOfWork)
        collection = self.ds.connection(COLLECTION_UNIT_OF_WORK)
        return collection.save(instance.document, safe=True)

    @thread_safe
    def insert(self, instance):
        """ inserts a unit of work into MongoDB.
        :raises DuplicateKeyError: if such record already exist """
        assert isinstance(instance, UnitOfWork)
        collection = self.ds.connection(COLLECTION_UNIT_OF_WORK)
        try:
            return collection.insert(instance.document, safe=True)
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
        return collection.remove(uow_id, safe=True)
