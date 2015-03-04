__author__ = 'Bohdan Mushkevych'

from db.model.raw_data import *
from db.model.single_session import SingleSession
from synergy.db.dao.base_dao import BaseDao
from constants import COLLECTION_SINGLE_SESSION


class SingleSessionDao(BaseDao):
    """ Thread-safe Data Access Object for single_session table/collection """

    def __init__(self, logger):
        super(SingleSessionDao, self).__init__(logger=logger,
                                               model_class=SingleSession,
                                               primary_key=[DOMAIN_NAME, SESSION_ID],
                                               collection_name=COLLECTION_SINGLE_SESSION)
