__author__ = 'Bohdan Mushkevych'

from threading import RLock

from synergy.db.manager import ds_manager
from synergy.db.model import job, base_model
from synergy.db.model.job import Job
from synergy.system.decorator import thread_safe
from synergy.system.time_qualifier import *
from synergy.scheduler.scheduler_constants import COLLECTION_JOB_HOURLY, COLLECTION_JOB_DAILY, \
    COLLECTION_JOB_MONTHLY, COLLECTION_JOB_YEARLY
from synergy.conf.process_context import ProcessContext


QUERY_GET_LIKE_TIMEPERIOD = \
    lambda timeperiod: {base_model.TIMEPERIOD: {'$regex': timeperiod}}

QUERY_GET_LIKE_TIMEPERIOD_AND_NOT_PROCESSED = \
    lambda timeperiod: {base_model.TIMEPERIOD: {'$regex': timeperiod},
                        job.STATE: {'$ne': job.STATE_PROCESSED}}


class JobDao(object):
    """ Thread-safe Data Access Object from job_XXX collection
    above, XXX could stand for hourly, daily, monthly, yearly """
    def __init__(self, logger):
        super(JobDao, self).__init__()
        self.logger = logger
        self.lock = RLock()
        self.ds = ds_manager.ds_factory(logger)

    @thread_safe
    def _get_job_collection(self, process_name):
        """jobs are stored in 4 collections: hourly, daily, monthly and yearly;
        method looks for the proper job_collection base on process TIME_QUALIFIER"""
        qualifier = ProcessContext.get_time_qualifier(process_name)

        if qualifier == QUALIFIER_HOURLY:
            collection = self.ds.connection(COLLECTION_JOB_HOURLY)
        elif qualifier == QUALIFIER_DAILY:
            collection = self.ds.connection(COLLECTION_JOB_DAILY)
        elif qualifier == QUALIFIER_MONTHLY:
            collection = self.ds.connection(COLLECTION_JOB_MONTHLY)
        elif qualifier == QUALIFIER_YEARLY:
            collection = self.ds.connection(COLLECTION_JOB_YEARLY)
        else:
            raise ValueError('Unknown time qualifier: %s for %s' % (qualifier, process_name))
        return collection

    @thread_safe
    def get_one(self, key, timeperiod):
        """ method finds job record and returns it to the caller"""
        collection = self._get_job_collection(key)
        document = collection.find_one({job.PROCESS_NAME: key, base_model.TIMEPERIOD: timeperiod})

        if document is None:
            raise LookupError('MongoDB has no job record in %s collection for (%s, %s)' % (collection, key, timeperiod))
        return Job(document)

    @thread_safe
    def get_all(self, collection_name, since=None):
        """ method returns all job records from a particular collection that are older than <since> """
        if since is None:
            query = {}
        else:
            query = {base_model.TIMEPERIOD: {'$gte': since}}
        collection = self.ds.connection(collection_name)

        cursor = collection.find(query)
        if cursor.count() == 0:
            raise LookupError('MongoDB has no job records in %s collection since %r' % (collection_name, since))
        return [Job(document) for document in cursor]

    @thread_safe
    def run_query(self, collection_name, query):
        """ method runs query on a specified collection and return a list of filtered Job records """
        cursor = self.ds.filter(collection_name, query)
        return [Job(document) for document in cursor]

    @thread_safe
    def update(self, instance):
        assert isinstance(instance, Job)
        collection = self._get_job_collection(instance.process_name)
        return collection.save(instance.document, safe=True)
