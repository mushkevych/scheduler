__author__ = 'Bohdan Mushkevych'

from model.raw_data import *
from model.single_session import SingleSessionStatistics
from system.collection_context import CollectionContext, COLLECTION_SINGLE_SESSION


def get_one(logger, domain_name, session_id):
    single_session_collection = CollectionContext.get_collection(logger, COLLECTION_SINGLE_SESSION)
    query = {DOMAIN_NAME: domain_name,
             FAMILY_USER_PROFILE + '.' + SESSION_ID: session_id}
    document = single_session_collection.find_one(query)
    if document is None:
        return None
    return SingleSessionStatistics(document)


def update(logger, instance, is_safe):
    """ method finds scheduler_configuration record and update its DB representation"""
    w_number = CollectionContext.get_w_number(logger, COLLECTION_SINGLE_SESSION)
    collection = CollectionContext.get_collection(logger, COLLECTION_SINGLE_SESSION)
    collection.save(instance.document, safe=is_safe, w=w_number)
