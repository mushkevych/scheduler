__author__ = 'Bohdan Mushkevych'

from synergy.db.dao.base_dao import BaseDao
from synergy.db.model.managed_process_entry import ManagedProcessEntry, PROCESS_NAME
from synergy.system.decorator import thread_safe
from synergy.scheduler.scheduler_constants import COLLECTION_MANAGED_PROCESS


class ManagedProcessDao(BaseDao):
    """ Thread-safe Data Access Object for managed_process table/collection """

    def __init__(self, logger):
        super(ManagedProcessDao, self).__init__(logger=logger,
                                                model_class=ManagedProcessEntry,
                                                primary_key=[PROCESS_NAME],
                                                collection_name=COLLECTION_MANAGED_PROCESS)

    @thread_safe
    def clear(self):
        """ removes all documents in this collection """
        collection = self.ds.connection(COLLECTION_MANAGED_PROCESS)
        return collection.delete_many(filter={})
