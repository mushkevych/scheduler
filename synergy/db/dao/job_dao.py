__author__ = 'Bohdan Mushkevych'

from threading import RLock

from bson import ObjectId

from synergy.conf import context
from synergy.db.manager import ds_manager
from synergy.db.model import job
from synergy.db.model.job import Job
from synergy.scheduler.scheduler_constants import COLLECTION_JOB_HOURLY, COLLECTION_JOB_DAILY, \
    COLLECTION_JOB_MONTHLY, COLLECTION_JOB_YEARLY
from synergy.system.decorator import thread_safe
from synergy.system.time_qualifier import *


QUERY_GET_LIKE_TIMEPERIOD = lambda timeperiod, include_running, include_processed, include_noop, include_failed: {
    job.TIMEPERIOD: {'$gte': timeperiod},
    job.STATE: {'$in': [job.STATE_PROCESSED if include_processed else None,
                        job.STATE_IN_PROGRESS if include_running else None,
                        job.STATE_EMBRYO if include_running else None,
                        job.STATE_FINAL_RUN if include_running else None,
                        job.STATE_SKIPPED if include_failed else None,
                        job.STATE_NOOP if include_noop else None]}
}


class JobDao(object):
    """ Thread-safe Data Access Object from job_XXX collection
    above, XXX could stand for hourly, daily, monthly, yearly """
    def __init__(self, logger):
        super(JobDao, self).__init__()
        self.logger = logger
        self.lock = RLock()
        self.ds = ds_manager.ds_factory(logger)

    @thread_safe
    def _get_job_collection_name(self, process_name):
        """jobs are stored in 4 collections: hourly, daily, monthly and yearly;
        method looks for the proper job_collection base on process TIME_QUALIFIER"""
        qualifier = context.process_context[process_name].time_qualifier

        if qualifier == QUALIFIER_HOURLY:
            collection_name = COLLECTION_JOB_HOURLY
        elif qualifier == QUALIFIER_DAILY:
            collection_name = COLLECTION_JOB_DAILY
        elif qualifier == QUALIFIER_MONTHLY:
            collection_name = COLLECTION_JOB_MONTHLY
        elif qualifier == QUALIFIER_YEARLY:
            collection_name = COLLECTION_JOB_YEARLY
        else:
            raise ValueError(f'Unknown time qualifier: {qualifier} for {process_name}')
        return collection_name

    @thread_safe
    def get_by_id(self, process_name, db_id):
        """ method finds a single job record and returns it to the caller"""
        collection_name = self._get_job_collection_name(process_name)
        collection = self.ds.connection(collection_name)
        document = collection.find_one({'_id': ObjectId(db_id)})

        if document is None:
            raise LookupError(f'MongoDB has no job record in collection {collection} for {db_id}')
        return Job.from_json(document)

    @thread_safe
    def get_one(self, process_name, timeperiod):
        """ method finds a single job record and returns it to the caller"""
        collection_name = self._get_job_collection_name(process_name)
        collection = self.ds.connection(collection_name)
        document = collection.find_one({job.PROCESS_NAME: process_name, job.TIMEPERIOD: timeperiod})

        if document is None:
            raise LookupError(f'MongoDB has no job record in collection {collection} for {process_name}@{timeperiod}')
        return Job.from_json(document)

    @thread_safe
    def get_all(self, collection_name, since=None):
        """ method returns all job records from a particular collection that are older than <since> """
        if since is None:
            query = {}
        else:
            query = {job.TIMEPERIOD: {'$gte': since}}
        collection = self.ds.connection(collection_name)

        cursor = collection.find(query)
        if cursor.count() == 0:
            raise LookupError(f'MongoDB has no job records in collection {collection_name} since {since}')
        return [Job.from_json(document) for document in cursor]

    @thread_safe
    def run_query(self, collection_name, query):
        """ method runs query on a specified collection and return a list of filtered Job records """
        cursor = self.ds.filter(collection_name, query)
        return [Job.from_json(document) for document in cursor]

    @thread_safe
    def update(self, instance):
        assert isinstance(instance, Job)
        collection_name = self._get_job_collection_name(instance.process_name)
        if instance.db_id:
            query = {'_id': ObjectId(instance.db_id)}
        else:
            query = {job.PROCESS_NAME: instance.process_name,
                     job.TIMEPERIOD: instance.timeperiod}
        self.ds.update(collection_name, query, instance)
        return instance.db_id
