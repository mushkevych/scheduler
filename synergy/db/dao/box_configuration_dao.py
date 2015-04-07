__author__ = 'Bohdan Mushkevych'

from synergy.db.dao.base_dao import BaseDao
from synergy.db.model.box_configuration import BoxConfiguration, BOX_ID, PROCESS_NAME
from synergy.supervisor.supervisor_constants import COLLECTION_BOX_CONFIGURATION

QUERY_PROCESSES_FOR_BOX_ID = lambda box_id: {BOX_ID: box_id}


class BoxConfigurationDao(BaseDao):
    """ Thread-safe Data Access Object for box_configuration table/collection """
    def __init__(self, logger):
        super(BoxConfigurationDao, self).__init__(logger=logger,
                                                  model_class=BoxConfiguration,
                                                  primary_key=[BOX_ID, PROCESS_NAME],
                                                  collection_name=COLLECTION_BOX_CONFIGURATION)
