__author__ = 'Bohdan Mushkevych'

import pymongo
from synergy.db.manager import ds_manager
from synergy.db.model.freerun_process_entry import ENTRY_NAME
from synergy.db.model.managed_process_entry import PROCESS_NAME, ManagedProcessEntry
from synergy.db.model.unit_of_work import TIMEPERIOD, START_ID, END_ID
from synergy.db.model.log_recording import PARENT_OBJECT_ID, CREATED_AT

from synergy.db.dao.managed_process_dao import ManagedProcessDao

from synergy.conf import context, settings
from synergy.scheduler.scheduler_constants import *
from synergy.system.system_logger import get_logger
from flow.db import db_manager


def synch_db():
    """ function reads managed_process and updates context entries appropriately """
    logger = get_logger(PROCESS_SCHEDULER)
    managed_process_dao = ManagedProcessDao(logger)

    try:
        process_entries = managed_process_dao.get_all()
    except LookupError:
        logger.error('Synergy DB is not initialized. Aborting.')
        exit(1)

    for process_entry in process_entries:
        process_name = process_entry.process_name
        if process_name not in context.process_context:
            logger.warning(f'Process {process_name} has no reflection in the context. Skipping it.')
            continue

        if not isinstance(context.process_context[process_name], ManagedProcessEntry):
            logger.error('Process entry {0} of non-managed type {1} found in managed_process table. Skipping it.'
                         .format(process_name, context.process_context[process_name].__class__.__name__))
            continue

        context.process_context[process_name] = process_entry
        logger.info(f'Context updated with process entry {process_entry.key}.')


def update_db():
    """ writes to managed_process table records from the context.process_context """
    logger = get_logger(PROCESS_SCHEDULER)
    managed_process_dao = ManagedProcessDao(logger)
    managed_process_dao.clear()

    for process_name, process_entry in context.process_context.items():
        if not isinstance(process_entry, ManagedProcessEntry):
            continue

        managed_process_dao.update(process_entry)
        logger.info(f'Updated DB with process entry {process_entry.key} from the context.')


def reset_db():
    """ drops the *scheduler* database, resets schema """
    logger = get_logger(PROCESS_SCHEDULER)
    logger.info('Starting *scheduler* DB reset')

    ds = ds_manager.ds_factory(logger)
    ds._db_client.drop_database(settings.settings['mongo_db_name'])
    logger.info('*scheduler* db has been dropped')

    connection = ds.connection(COLLECTION_MANAGED_PROCESS)
    connection.create_index([(PROCESS_NAME, pymongo.ASCENDING)], unique=True)

    connection = ds.connection(COLLECTION_FREERUN_PROCESS)
    connection.create_index([(PROCESS_NAME, pymongo.ASCENDING), (ENTRY_NAME, pymongo.ASCENDING)], unique=True)

    connection = ds.connection(COLLECTION_UNIT_OF_WORK)
    connection.create_index([(PROCESS_NAME, pymongo.ASCENDING),
                             (TIMEPERIOD, pymongo.ASCENDING),
                             (START_ID, pymongo.ASCENDING),
                             (END_ID, pymongo.ASCENDING)], unique=True)

    connection = ds.connection(COLLECTION_LOG_RECORDING)
    connection.create_index([(PARENT_OBJECT_ID, pymongo.ASCENDING)], unique=True)

    # expireAfterSeconds: <int> Used to create an expiring (TTL) collection.
    # MongoDB will automatically delete documents from this collection after <int> seconds.
    # The indexed field must be a UTC datetime or the data will not expire.
    ttl_seconds = settings.settings['db_log_ttl_days'] * 86400     # number of seconds for TTL
    connection.create_index(CREATED_AT, expireAfterSeconds=ttl_seconds)

    for collection_name in [COLLECTION_JOB_HOURLY, COLLECTION_JOB_DAILY,
                            COLLECTION_JOB_MONTHLY, COLLECTION_JOB_YEARLY]:
        connection = ds.connection(collection_name)
        connection.create_index([(PROCESS_NAME, pymongo.ASCENDING), (TIMEPERIOD, pymongo.ASCENDING)], unique=True)

    # reset Synergy Flow tables
    db_manager.reset_db()
    logger.info('*scheduler* db has been recreated')


if __name__ == '__main__':
    pass
