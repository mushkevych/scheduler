__author__ = 'Bohdan Mushkevych'

from threading import RLock
from synergy.db.manager import ds_manager
from synergy.db.model import box_configuration
from synergy.db.model.box_configuration import BoxConfiguration
from synergy.supervisor.supervisor_constants import COLLECTION_BOX_CONFIGURATION
from synergy.system.decorator import thread_safe


class BoxConfigurationDao(object):
    """ Thread-safe Data Access Object for box_configuration table/collection """
    def __init__(self, logger):
        super(BoxConfigurationDao, self).__init__()
        self.logger = logger
        self.lock = RLock()
        self.ds = ds_manager.ds_factory(logger)

    @thread_safe
    def get_one(self, key):
        """ method reads box configuration from the MongoDB"""
        collection = self.ds.connection(COLLECTION_BOX_CONFIGURATION)
        document = collection.find_one({box_configuration.BOX_ID: key})
        if document is None:
            raise LookupError('MongoDB has no process list for box_id = %r' % key)
        return BoxConfiguration(document)

    @thread_safe
    def update(self, instance):
        """ method updates box configuration in the MongoDB"""
        assert isinstance(instance, BoxConfiguration)
        collection = self.ds.connection(COLLECTION_BOX_CONFIGURATION)
        return collection.save(instance.document, safe=True)
