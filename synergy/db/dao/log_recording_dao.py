__author__ = 'Bohdan Mushkevych'

from synergy.db.dao.base_dao import BaseDao
from synergy.db.model.log_recording import LogRecording, PARENT_OBJECT_ID, LOG
from synergy.system.decorator import thread_safe
from synergy.scheduler.scheduler_constants import COLLECTION_LOG_RECORDING


class LogRecordingDao(BaseDao):
    """ Thread-safe Data Access Object for logs stored in log_recording table/collection """

    def __init__(self, logger):
        super(LogRecordingDao, self).__init__(logger=logger,
                                              model_class=LogRecording,
                                              primary_key=[PARENT_OBJECT_ID],
                                              collection_name=COLLECTION_LOG_RECORDING)

    @thread_safe
    def append_log(self, uow_id, msg):
        collection = self.ds.connection(self.collection_name)

        result = collection.update_one({PARENT_OBJECT_ID: uow_id},
                                       {'$push': {LOG: msg}},
                                       upsert=True)
        if result.modified_count == 0:
            raise LookupError(f'Log append failed for {uow_id} in collection {self.collection_name}')
