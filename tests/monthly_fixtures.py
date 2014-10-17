__author__ = 'Bohdan Mushkevych'

import base_fixtures
from constants import COLLECTION_SITE_MONTHLY
from synergy.system.time_qualifier import QUALIFIER_MONTHLY


# pylint: disable=C0301

# pylint: enable=C0301


def generated_site_entries():
    return base_fixtures.create_site_stats(COLLECTION_SITE_MONTHLY, QUALIFIER_MONTHLY)


def clean_site_entries():
    return base_fixtures.clean_site_entries(COLLECTION_SITE_MONTHLY, QUALIFIER_MONTHLY)


if __name__ == '__main__':
    pass