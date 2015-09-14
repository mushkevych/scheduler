__author__ = 'Bohdan Mushkevych'

import inspect
import random
from datetime import datetime, timedelta

from db.model import raw_data
from db.model.site_statistics import SiteStatistics
from db.model.single_session import SingleSession
from db.dao.single_session_dao import SingleSessionDao
from synergy.db.dao.unit_of_work_dao import UnitOfWorkDao
from synergy.db.model import unit_of_work
from synergy.db.model.unit_of_work import UnitOfWork
from synergy.db.model.synergy_mq_transmission import SynergyMqTransmission
from synergy.db.manager import ds_manager
from synergy.system import time_helper
from synergy.system.time_qualifier import *
from synergy.conf import context
from synergy.system.data_logging import get_logger
from tests.ut_context import PROCESS_UNIT_TEST


TOTAL_ENTRIES = 101


class TestMessage(object):
    """ mock class substituting MQ Message. Used for Unit Tests only """
    def __init__(self, process_name=None, uow_id=None):
        mq_request = SynergyMqTransmission(process_name=process_name, unit_of_work_id=uow_id)
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
            assert False, 'key %r: actual %r vs expected %r' % (expected_key, actual_value, expected_value)


def create_unit_of_work(process_name,
                        start_id,
                        end_id,
                        timeperiod='INVALID_TIMEPERIOD',
                        state=unit_of_work.STATE_REQUESTED,
                        created_at=datetime.utcnow(),
                        submitted_at=datetime.utcnow(),
                        uow_id=None):
    """ method creates and returns unit_of_work """
    try:
        source_collection = context.process_context[process_name].source
        sink_collection = context.process_context[process_name].sink
    except KeyError:
        source_collection = None
        sink_collection = None

    uow = UnitOfWork()
    uow.process_name = process_name
    uow.timeperiod = timeperiod
    uow.start_id = start_id
    uow.end_id = end_id
    uow.start_timeperiod = timeperiod
    uow.end_timeperiod = timeperiod
    uow.created_at = created_at
    uow.submitted_at = submitted_at
    uow.source = source_collection
    uow.sink = sink_collection
    uow.state = state
    uow.unit_of_work_type = unit_of_work.TYPE_MANAGED
    uow.number_of_retries = 0
    uow.arguments = context.process_context[process_name].arguments

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
    random.seed(seed)
    object_ids = []
    for i in range(TOTAL_ENTRIES):
        key = composite_key_function(i, TOTAL_ENTRIES)
        session = SingleSession()
        session.key = (key[0], key[1], 'session_id_%s' % str(i))
        session.ip = '192.168.0.2'
        if i % 3 == 0:
            session.user_profile.screen_res = (240, 360)
        elif i % 5 == 0:
            session.user_profile.screen_res = (360, 480)
        else:
            session.user_profile.screen_res = (760, 980)

        if i % 2 == 0:
            session.user_profile.os = 'Linux'
            session.user_profile.browser = 'FF %s' % str(i % 4)
            session.user_profile.language = 'en_ca'
            session.user_profile.country = 'ca'
        else:
            session.user_profile.os = 'Windows'
            session.user_profile.browser = 'IE %s' % str(i % 9)
            session.user_profile.language = 'ua_uk'
            session.user_profile.country = 'eu'

        session.browsing_history.total_duration = random.randint(0, 200)
        session.browsing_history.number_of_pageviews = random.randint(1, 5)

        for index in range(random.randint(1, 4)):
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

    return 'domain_name_%s' % str(index - iteration_index * 33), iteration_timeperiod


def create_site_stats(collection_name, time_qualifier, seed='RANDOM_SEED_OBJECT'):
    logger = get_logger(PROCESS_UNIT_TEST)
    ds = ds_manager.ds_factory(logger)
    random.seed(seed)
    object_ids = []
    for i in range(TOTAL_ENTRIES):
        key = generate_site_composite_key(i, time_qualifier)
        site_stat = SiteStatistics()
        site_stat.key = key
        site_stat.stat.number_of_visits = random.randint(1, 1000)
        site_stat.stat.total_duration = random.randint(0, 100)

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

        stat_id = ds.insert(collection_name, site_stat.document)
        object_ids.append(stat_id)

    return object_ids


def clean_site_entries(collection_name, time_qualifier):
    logger = get_logger(PROCESS_UNIT_TEST)
    ds = ds_manager.ds_factory(logger)
    connection = ds.connection(collection_name)
    for i in range(TOTAL_ENTRIES):
        key = generate_site_composite_key(i, time_qualifier)
        connection.remove({raw_data.DOMAIN_NAME: key[0], raw_data.TIMEPERIOD: key[1]})


def wind_actual_timeperiod(new_time):
    """ method is used to overload actual_timeperiod method from the time_helper """
    def actual_timeperiod(time_qualifier):
        return time_helper.datetime_to_synergy(time_qualifier, new_time)

    return actual_timeperiod


def wind_the_time(time_qualifier, timeperiod, delta):
    """ method is used to calculate new timeperiod, shifted by number of units (hours or days)"""
    pattern = time_helper.define_pattern(timeperiod)
    t = datetime.strptime(timeperiod, pattern)

    if time_qualifier == QUALIFIER_HOURLY:
        t = t + timedelta(hours=delta)
        return t.strftime('%Y%m%d%H')
    elif time_qualifier == QUALIFIER_DAILY:
        t = t + timedelta(days=delta)
        return t.strftime('%Y%m%d00')
    raise ValueError('unsupported time_qualifier')


if __name__ == '__main__':
    pass
