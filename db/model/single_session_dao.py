from db.manager import ds_manager

__author__ = 'Bohdan Mushkevych'

from db.model.raw_data import *
from db.model.single_session import SingleSessionStatistics
from system.collection_context import COLLECTION_SINGLE_SESSION


def get_one(logger, domain_name, session_id):
    ds = ds_manager.ds_factory(logger)
    collection = ds.connection(COLLECTION_SINGLE_SESSION)

    query = {DOMAIN_NAME: domain_name,
             FAMILY_USER_PROFILE + '.' + SESSION_ID: session_id}
    document = collection.find_one(query)
    if document is None:
        raise LookupError('MongoDB has no single session record for (%s, %s)' % (domain_name, session_id))
    return SingleSessionStatistics(document)


def update(logger, instance, is_safe):
    """ method finds scheduler_configuration record and update its DB representation"""
    ds = ds_manager.ds_factory(logger)
    collection = ds.connection(COLLECTION_SINGLE_SESSION)
    collection.save(instance.document, safe=is_safe)
