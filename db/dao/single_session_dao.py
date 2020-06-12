__author__ = 'Bohdan Mushkevych'

from db.model.single_session import SingleSession
from synergy.db.dao.base_dao import BaseDao
from constants import COLLECTION_SINGLE_SESSION

SESSION_ID = SingleSession.session_id.name
DOMAIN_NAME = SingleSession.domain_name.name


class SingleSessionDao(BaseDao):
    """ Thread-safe Data Access Object for single_session table/collection """

    def __init__(self, logger):
        super(SingleSessionDao, self).__init__(logger=logger,
                                               collection_name=COLLECTION_SINGLE_SESSION,
                                               model_class=SingleSession)

    def find_by_session_id(self, domain_name, session_id):
        query = {DOMAIN_NAME: domain_name, SESSION_ID: session_id}
        sessions = self.run_query(query)
        return sessions[0]
