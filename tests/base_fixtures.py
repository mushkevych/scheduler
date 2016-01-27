__author__ = 'Bohdan Mushkevych'

import inspect
from datetime import datetime

from db.model.site_statistics import SiteStatistics
from db.model.single_session import SingleSession
from db.dao.single_session_dao import SingleSessionDao
from db.dao.site_dao import SiteDao
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.model import unit_of_work
from synergy.db.model.unit_of_work import UnitOfWork
from synergy.db.model.mq_transmission import MqTransmission
from synergy.system import time_helper
from synergy.conf import context
from synergy.system.system_logger import get_logger
from tests.mt19937 import MT19937
from tests.ut_context import PROCESS_UNIT_TEST


TOTAL_ENTRIES = 101


class TestMessage(object):
    """ mock class substituting MQ Message. Used for Unit Tests only """
    def __init__(self, process_name=None, uow_id=None):
        mq_request = MqTransmission(process_name=process_name, record_db_id=uow_id)
        self.body = mq_request.to_json()
        self.delivery_tag = None


def get_field_starting_with(prefix, module):
    """method reads Python module and iterates thru all its fields
    Those that are starting with defined prefix are returned as list
    :param prefix: define prefix. For example EXPECTED_YEARLY_TEMPLATE
    :param module: defines fully qualified name of the Python module. For example tests.yearly_fixtures"""
    fields = []
    for name, value in inspect.getmembers(module):
        if name.startswith(prefix):
            fields.append(value)

    return fields


def compare_dictionaries(dict_actual, dict_expected):
    """ method compares two presumably identical dictionaries
    @raise assert exception: in case two dictionaries are _not_ identical """
    for expected_key in dict_expected:
        expected_value = dict_expected[expected_key]
        actual_value = dict_actual.get(expected_key)
        if isinstance(expected_value, dict):
            compare_dictionaries(expected_value, actual_value)
        elif isinstance(expected_value, list):
            if isinstance(actual_value, set):
                actual_value = list(actual_value)
            assert actual_value.sort() == expected_value.sort()
        elif actual_value != expected_value:
            assert False, 'actual vs expected for {0}: {1} vs {2}'.format(expected_key, actual_value, expected_value)


def create_unit_of_work(process_name,
                        start_id,
                        end_id,
                        timeperiod='INVALID_TIMEPERIOD',
                        state=unit_of_work.STATE_REQUESTED,
                        created_at=datetime.utcnow(),
                        submitted_at=datetime.utcnow(),
                        uow_id=None):
    """ method creates and returns unit_of_work """
    process_entry = context.process_context[process_name]

    uow = UnitOfWork()
    uow.process_name = process_name
    uow.timeperiod = timeperiod
    uow.start_id = start_id
    uow.end_id = end_id
    uow.start_timeperiod = timeperiod
    uow.end_timeperiod = timeperiod
    uow.created_at = created_at
    uow.submitted_at = submitted_at
    uow.source = process_entry.source if hasattr(process_entry, 'source') else None
    uow.sink = process_entry.sink if hasattr(process_entry, 'sink') else None
    uow.state = state
    uow.unit_of_work_type = unit_of_work.TYPE_MANAGED
    uow.number_of_retries = 0
    uow.arguments = process_entry.arguments

    if uow_id is not None:
        uow.db_id = uow_id

    return uow


def create_and_insert_unit_of_work(process_name, start_id, end_id, state=unit_of_work.STATE_REQUESTED,
                                   timeperiod='INVALID_TIMEPERIOD'):
    """ method creates and inserts a unit_of_work into DB
    :return id of the created object in the db"""
    uow = create_unit_of_work(process_name, start_id, end_id, timeperiod, state)
    logger = get_logger(process_name)
    uow_dao = UnitOfWorkDao(logger)
    uow_id = uow_dao.insert(uow)
    return uow_id


def create_session_stats(composite_key_function, seed='RANDOM_SEED_OBJECT'):
    logger = get_logger(PROCESS_UNIT_TEST)
    ss_dao = SingleSessionDao(logger)
    time_array = ['20010303102210', '20010303102212', '20010303102215', '20010303102250']
    rnd = MT19937(seed)
    object_ids = []
    for i in range(TOTAL_ENTRIES):
        session = SingleSession()
        session.key = composite_key_function(i, TOTAL_ENTRIES)
        session.ip = '192.168.0.2'
        if i % 3 == 0:
            session.user_profile.screen_res = (240, 360)
        elif i % 5 == 0:
            session.user_profile.screen_res = (360, 480)
        else:
            session.user_profile.screen_res = (760, 980)

        if i % 2 == 0:
            session.user_profile.os = 'Linux'
            session.user_profile.browser = 'FF {0}'.format(i % 4)
            session.user_profile.language = 'en_ca'
            session.user_profile.country = 'ca'
        else:
            session.user_profile.os = 'Windows'
            session.user_profile.browser = 'IE {0}'.format(i % 9)
            session.user_profile.language = 'ua_uk'
            session.user_profile.country = 'eu'

        session.browsing_history.total_duration = rnd.extract_number()
        session.browsing_history.number_of_pageviews = rnd.extract_number()

        for index in range(4):
            session.browsing_history.number_of_entries = index + 1
            session.browsing_history.set_entry_timestamp(index, time_array[index])

        sess_id = ss_dao.update(session)
        object_ids.append(sess_id)

    return object_ids


def _generate_entries(token, number, value):
    items = dict()
    for i in range(number):
        items[token + str(i)] = value
    return items


def generate_site_composite_key(index, time_qualifier):
    start_time = '20010303101010'  # YYYYMMDDHHmmSS

    iteration_index = index // 33  # number larger than number of hours in a day and days in a month
    iteration_timeperiod = time_helper.cast_to_time_qualifier(time_qualifier, start_time)
    if iteration_index:
        iteration_timeperiod = time_helper.increment_timeperiod(time_qualifier,
                                                                iteration_timeperiod,
                                                                delta=iteration_index)

    return 'domain_name_{0}'.format(index - iteration_index * 33), iteration_timeperiod


def create_site_stats(collection_name, time_qualifier, seed='RANDOM_SEED_OBJECT'):
    logger = get_logger(PROCESS_UNIT_TEST)
    site_dao = SiteDao(logger)
    rnd = MT19937(seed)
    object_ids = []
    for i in range(TOTAL_ENTRIES):
        key = generate_site_composite_key(i, time_qualifier)
        site_stat = SiteStatistics()
        site_stat.key = key
        site_stat.stat.number_of_visits = rnd.extract_number()
        site_stat.stat.total_duration = rnd.extract_number()

        items = _generate_entries('os_', 5, i)
        site_stat.stat.os = items

        items = _generate_entries('browser_', 5, i)
        site_stat.stat.browsers = items

        items = dict()
        items['(320, 240)'] = 3
        items['(640, 480)'] = 5
        items['(1024, 960)'] = 7
        items['(1280, 768)'] = 9
        site_stat.stat.screen_res = items

        items = dict()
        items['ca_en'] = 3
        items['ca_fr'] = 5
        items['ua_uk'] = 7
        items['us_en'] = 9
        site_stat.stat.languages = items

        items = dict()
        items['ca'] = 3
        items['fr'] = 5
        items['uk'] = 7
        items['us'] = 9
        site_stat.stat.countries = items

        stat_id = site_dao.insert(collection_name, site_stat)
        object_ids.append(stat_id)

    return object_ids


def clean_site_entries(collection_name, time_qualifier):
    logger = get_logger(PROCESS_UNIT_TEST)
    site_dao = SiteDao(logger)
    for i in range(TOTAL_ENTRIES):
        key = generate_site_composite_key(i, time_qualifier)
        site_dao.remove(collection_name, key[0], key[1])


if __name__ == '__main__':
    pass
