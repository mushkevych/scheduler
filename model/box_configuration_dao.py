__author__ = 'Bohdan Mushkevych'

from model import box_configuration
from model.box_configuration import BoxConfiguration
from model import ds_manager
from system.collection_context import COLLECTION_BOX_CONFIGURATION


def get_one(logger, key):
    """ method reads box configuration from the MongoDB"""
    ds = ds_manager.ds_factory(logger)
    collection = ds.connection(COLLECTION_BOX_CONFIGURATION)
    document = collection.find_one({box_configuration.BOX_ID: key})
    if document is None:
        raise LookupError('MongoDB has no process list for box_id = %r' % key)
    return BoxConfiguration(document)


def update(logger, instance):
    """ method updates box configuration in the MongoDB"""
    ds = ds_manager.ds_factory(logger)
    collection = ds.connection(COLLECTION_BOX_CONFIGURATION)
    collection.save(instance.document, safe=True)
