__author__ = 'Bohdan Mushkevych'

import pymongo
from synergy.db.manager import ds_manager
from synergy.db.model.freerun_process_entry import ENTRY_NAME
from synergy.db.model.managed_process_entry import PROCESS_NAME
from synergy.db.model.unit_of_work import TIMEPERIOD, START_OBJ_ID, END_OBJ_ID

from synergy.db.dao.managed_process_dao import ManagedProcessDao

from synergy.conf import context, settings
from synergy.scheduler.scheduler_constants import *
from synergy.system.data_logging import get_logger


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
            logger.error('Process %r has no reflection in the context. Skipping it.' % process_name)
            continue

        process_type = context.process_context[process_name].process_type
        if process_type not in [TYPE_MANAGED, TYPE_GARBAGE_COLLECTOR]:
            logger.error('Process type %s of %s should not be reflected in the managed_process table. Skipping it.'
                         % (process_type, process_name))
            continue

        context.process_context[process_name] = process_entry
        logger.info('Context updated with process entry %s.' % process_entry.key)


def init_db():
    """ writes to managed_process table records from the context.process_context """
    logger = get_logger(PROCESS_SCHEDULER)
    managed_process_dao = ManagedProcessDao(logger)
    managed_process_dao.clear()

    for process_name, process_entry in context.process_context.items():
        if process_entry.process_type != TYPE_MANAGED:
            continue

        managed_process_dao.update(process_entry)
        logger.info('Updated DB with process entry %s from the context.' % process_entry.key)


def flush_db():
    """ drops the *scheduler* database, resets schema """
    logger = get_logger(PROCESS_SCHEDULER)
    logger.info('Starting *scheduler* DB flush')

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
                             (START_OBJ_ID, pymongo.ASCENDING),
                             (END_OBJ_ID, pymongo.ASCENDING)], unique=True)

    for collection_name in [COLLECTION_JOB_HOURLY, COLLECTION_JOB_DAILY,
                            COLLECTION_JOB_MONTHLY, COLLECTION_JOB_YEARLY]:
        connection = ds.connection(collection_name)
        connection.create_index([(PROCESS_NAME, pymongo.ASCENDING), (TIMEPERIOD, pymongo.ASCENDING)], unique=True)
    logger.info('*scheduler* db has been recreated')


if __name__ == '__main__':
    pass
