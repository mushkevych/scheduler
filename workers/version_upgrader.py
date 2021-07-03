__author__ = 'Bohdan Mushkevych'

from synergy.db.model import unit_of_work
from synergy.workers.abstract_uow_aware_worker import AbstractUowAwareWorker


EXTERNAL_VERSION_FILE = '/etc/some_application/version'


def get_version(logger) -> str:
    """ retrieves version from the `EXTERNAL_VERSION_FILE` file
        :raise EnvironmentError: if the version file could not be opened
    """
    try:
        with open(EXTERNAL_VERSION_FILE) as a_file:
            for a_line in a_file:
                a_line = a_line.strip()
                if a_line.startswith('#'):
                    continue
                return a_line

    except EnvironmentError:  # parent of IOError, OSError, FileNotFoundError
        logger.error(f'Can not read version file at: {EXTERNAL_VERSION_FILE}', exc_info=True)
        raise


class VersionUpgrader(AbstractUowAwareWorker):
    """ example worker: associated with a time_qualifier.QUALIFIER_ONCE
        and is meant to be run once to perform between-versions updates
    """

    def __init__(self, process_name):
        super(VersionUpgrader, self).__init__(process_name, perform_db_logging=True)

    def __del__(self):
        super(VersionUpgrader, self).__del__()

    def _process_uow(self, uow):
        version = get_version(self.logger)
        if '1' < version < '2':
            print('Running data jobs to upgrade from version 1 to 2')
        if '2' < version < '3':
            print('Running data jobs to upgrade from version 2 to 3')
        return 0, unit_of_work.STATE_PROCESSED
