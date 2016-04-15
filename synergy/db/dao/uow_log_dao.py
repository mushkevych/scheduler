__author__ = 'Bohdan Mushkevych'

from synergy.db.dao.base_dao import BaseDao
from synergy.db.model.uow_log_entry import UowLogEntry, RELATED_UNIT_OF_WORK, LOG
from synergy.scheduler.scheduler_constants import COLLECTION_UOW_LOG


class UowLogDao(BaseDao):
    """ Thread-safe Data Access Object for uow_log table/collection """

    def __init__(self, logger):
        super(UowLogDao, self).__init__(logger=logger,
                                        model_class=UowLogEntry,
                                        primary_key=[RELATED_UNIT_OF_WORK],
                                        collection_name=COLLECTION_UOW_LOG)

    def append_log(self, uow_id, record):
        collection = self.ds.connection(self.collection_name)

        result = collection.update_one({RELATED_UNIT_OF_WORK: uow_id},
                                       {'$push': {LOG: record}},
                                       upsert=True)
        if result.modified_count == 0:
            raise LookupError('Log append failed for {0} in collection {1}'.format(uow_id, self.collection_name))
