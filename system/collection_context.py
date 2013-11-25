from db.manager import ds_manager

__author__ = 'Bohdan Mushkevych'

COLLECTION_SINGLE_SESSION = 'single_session'
COLLECTION_SCHEDULER_CONFIGURATION = 'scheduler_configuration'
COLLECTION_UNITS_OF_WORK = 'units_of_work'
COLLECTION_BOX_CONFIGURATION = 'box_configuration'

COLLECTION_TIMETABLE_HOURLY = 'timetable_hourly'
COLLECTION_TIMETABLE_DAILY = 'timetable_daily'
COLLECTION_TIMETABLE_MONTHLY = 'timetable_monthly'
COLLECTION_TIMETABLE_YEARLY = 'timetable_yearly'


class CollectionContext:
    @classmethod
    def get_collection(cls, logger, collection_name):
        """ method retrieves connection from Data Source Manager
        Returns fully specified connection to collection.
        Avoid pooling at this level, as it blocks ClusterConnection load balancing"""
        try:
            ds = ds_manager.ds_factory(logger)
            return ds.connection(collection_name)
        except Exception:
            logger.error('CollectionContext error: %s' % collection_name, exc_info=True)