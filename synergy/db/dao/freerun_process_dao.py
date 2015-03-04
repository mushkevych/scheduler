__author__ = 'Bohdan Mushkevych'

from synergy.db.dao.base_dao import BaseDao
from synergy.db.model.freerun_process_entry import FreerunProcessEntry, PROCESS_NAME, ENTRY_NAME
from synergy.scheduler.scheduler_constants import COLLECTION_FREERUN_PROCESS


class FreerunProcessDao(BaseDao):
    """ Thread-safe Data Access Object for freerun_process table/collection """

    def __init__(self, logger):
        super(FreerunProcessDao, self).__init__(logger=logger,
                                                model_class=FreerunProcessEntry,
                                                primary_key=[PROCESS_NAME, ENTRY_NAME],
                                                collection_name=COLLECTION_FREERUN_PROCESS)
