__author__ = 'Bohdan Mushkevych'

import inspect
import random
from datetime import datetime, timedelta

from db.manager import ds_manager
from db.dao.single_session_dao import SingleSessionDao
from db.dao.unit_of_work_dao import UnitOfWorkDao
from db.model import unit_of_work
from db.model.unit_of_work import UnitOfWork
from db.model.single_session import SingleSession
from system import time_helper
from system.process_context import ProcessContext, PROCESS_UNIT_TEST

TOTAL_ENTRIES = 101


class TestConsumer(object):
    """ empty class that should substitute MQ Flopsy Consumer. Used for testing only """

    def acknowledge(self, delivery_tag):
        pass

    def close(self):
        pass


class TestMessage(object):
    """ empty class that should substitute MQ Message. Used for testing only """

    def __init__(self):
        self.body = None
        self.delivery_tag = None


class TestSiteMembershipDictionary(dict):
    """ this dictionary is used for testing period only to stub Synergy Construction replies"""

    def __init__(self, array_of_sites):
        super(TestSiteMembershipDictionary, self).__init__()
        random.seed('RANDOM_SEED_OBJECT')
        for site in array_of_sites:
            super(TestSiteMembershipDictionary, self).__setitem__(site, ['portfolio_%d' % random.randint(0, 20)])
#            print 'key %s value %s len %s' % ( site, self[site],  self.__len__())


class TestRestClient():
    def __init__(self, logger):
        self.logger = logger

    def get_group_mapping(self, timeperiod, list_of_sites):
        return TestSiteMembershipDictionary(list_of_sites)

    def get_client_mapping(self, timeperiod, list_of_sites):
        return TestSiteMembershipDictionary(list_of_sites)

    def get_portfolio_mapping(self, timeperiod, list_of_sites):
        return TestSiteMembershipDictionary(list_of_sites)

    def get_list_of_sci(self, timeperiod):
        return ['client_id_0', 'client_id_1', 'client_id_2', 'client_id_3']


def get_field_starting_with(prefix, module):
    """method reads Python module and iterates thru all its fields
    Those that are starting with defined prefix are returned as list
    @param prefix: define prefix. For example EXPECTED_YEARLY_TEMPLATE
    @param module: defines fully qualified name of the Python module. For example tests.yearly_fixtures"""
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
            assert 1 == 0, 'key %r: actual %r vs expected %r' % (expected_key, actual_value, expected_value)


def create_unit_of_work(process_name,
                        first_object_id,
                        last_object_id,
                        timeperiod='INVALID_TIMEPERIOD',
                        state=unit_of_work.STATE_REQUESTED,
                        creation_at=datetime.utcnow(),
                        uow_id=None):
    """ method creates and returns unit_of_work """
    try:
        source_collection = ProcessContext.get_source_collection(process_name)
        target_collection = ProcessContext.get_target_collection(process_name)
    except KeyError:
        source_collection = None
        target_collection = None

    uow = UnitOfWork()
    uow.timeperiod = timeperiod
    uow.start_timeperiod = timeperiod
    uow.end_timeperiod = timeperiod
    uow.start_id = first_object_id
    uow.end_id = last_object_id
    uow.source_collection = source_collection
    uow.target_collection = target_collection
    uow.state = state
    uow.created_at = creation_at
    uow.process_name = process_name
    uow.number_of_retries = 0

    if uow_id is not None:
        uow.document['_id'] = uow_id

    return uow


def create_and_insert_unit_of_work(process_name, first_object_id, last_object_id, timeperiod='INVALID_TIMEPERIOD'):
    """ method creates and inserts a unit_of_work into DB
    :return id of the created object in the db"""
    uow = create_unit_of_work(process_name, first_object_id, last_object_id, timeperiod)
    logger = ProcessContext.get_logger(process_name)
    uow_dao = UnitOfWorkDao(logger)
    uow_id = uow_dao.insert(uow)
    return uow_id


def create_session_stats(composite_key_function, seed='RANDOM_SEED_OBJECT'):
    logger = ProcessContext.get_logger(PROCESS_UNIT_TEST)
    ss_dao = SingleSessionDao(logger)
    time_array = ['20010303102210', '20010303102212', '20010303102215', '20010303102250']
    random.seed(seed)
    object_ids = []
    for i in range(TOTAL_ENTRIES):
        key = composite_key_function(i, TOTAL_ENTRIES)
        session = SingleSession()
        session.key = (key[0], key[1])
        session.session_id = 'session_id_%s' % str(i)
        session.ip = '192.168.0.2'
        if i % 3 == 0:
            session.screen_res = (240, 360)
        elif i % 5 == 0:
            session.screen_res = (360, 480)
        else:
            session.screen_res = (760, 980)

        if i % 2 == 0:
            session.os = 'Linux'
            session.browser = 'FF %s' % str(i % 4)
            session.language = 'en_ca'
            session.country = 'ca'
        else:
            session.os = 'Windows'
            session.browser = 'IE %s' % str(i % 9)
            session.language = 'ua_uk'
            session.country = 'eu'

        session.total_duration = random.randint(0, 200)
        session.number_of_pageviews = random.randint(1, 5)

        for index in range(random.randint(1, 4)):
            session.number_of_entries = index + 1
            session.set_entry_timestamp(index, time_array[index])

        sess_id = ss_dao.insert(session)
        object_ids.append(sess_id)

    return object_ids


def _generate_entries(token, number, value):
    items = dict()
    for i in range(number):
        items[token + str(i)] = value
    return items


def create_site_stats(collection, composite_key_function, statistics_klass, seed='RANDOM_SEED_OBJECT'):
    logger = ProcessContext.get_logger(PROCESS_UNIT_TEST)
    ds = ds_manager.ds_factory(logger)
    random.seed(seed)
    object_ids = []
    for i in range(TOTAL_ENTRIES):
        key = composite_key_function(i, TOTAL_ENTRIES)
        site_stat = statistics_klass()
        site_stat.key = (key[0], key[1])
        site_stat.number_of_visits = random.randint(1, 1000)
        site_stat.total_duration = random.randint(0, 100)

        items = _generate_entries('os_', 5, i)
        site_stat.os = items

        items = _generate_entries('browser_', 5, i)
        site_stat.browsers = items

        items = dict()
        items['(320, 240)'] = 3
        items['(640, 480)'] = 5
        items['(1024, 960)'] = 7
        items['(1280, 768)'] = 9
        site_stat.screen_res = items

        items = dict()
        items['ca_en'] = 3
        items['ca_fr'] = 5
        items['ua_uk'] = 7
        items['us_en'] = 9
        site_stat.languages = items

        items = dict()
        items['ca'] = 3
        items['fr'] = 5
        items['uk'] = 7
        items['us'] = 9
        site_stat.countries = items

        stat_id = ds.insert(site_stat.document)
        object_ids.append(stat_id)

    return object_ids


def wind_actual_timeperiod(new_time):
    """ method is used to overload actual_timeperiod method from the time_helper """
    def actual_timeperiod(time_qualifier):
        return time_helper.datetime_to_synergy(time_qualifier, new_time)

    return actual_timeperiod


def wind_the_time(time_qualifier, timeperiod, delta):
    """ method is used to calculate new timeperiod, shifted by number of units (hours or days)"""
    pattern = time_helper.define_pattern(timeperiod)
    t = datetime.strptime(timeperiod, pattern)

    if time_qualifier == ProcessContext.QUALIFIER_HOURLY:
        t = t + timedelta(hours=delta)
        return t.strftime('%Y%m%d%H')
    elif time_qualifier == ProcessContext.QUALIFIER_DAILY:
        t = t + timedelta(days=delta)
        return t.strftime('%Y%m%d00')
    raise ValueError('unsupported time_qualifier')



if __name__ == '__main__':
    pass
