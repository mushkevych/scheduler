__author__ = 'Bohdan Mushkevych'

import atexit

from synergy.conf import settings
from synergy.db.manager.ds_manager import MongoDbManager, HBaseManager


class DataSource:
    instances = dict()

    @classmethod
    def instance(cls, logger):
        id_logger = id(logger)  # allow one DB Connection per process (hence - per logger)
        if id_logger not in DataSource.instances:
            ds_type = settings.settings['ds_type']
            if ds_type == "mongo_db":
                ds = MongoDbManager(logger)
            elif ds_type == "hbase":
                ds = HBaseManager(logger)
            else:
                raise ValueError(f'Unsupported Data Source type: {ds_type}')

            atexit.register(ds.interpreter_terminating)
            DataSource.instances[id_logger] = ds

        return DataSource.instances[id_logger]

    @classmethod
    def _cleanup(cls):
        """ method is registered with the atexit hook, and ensures all MongoDb connections are closed """
        for _, ds in DataSource.instances.items():
            try:
                ds.__del__()
            except Exception as e:
                print(f'{e}')


# ensure the DB Connections are closed on the exit
atexit.register(DataSource._cleanup)


def get_data_source(logger):
    return DataSource.instance(logger)
