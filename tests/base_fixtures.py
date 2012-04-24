"""
Created on 2011-02-25

@author: Bohdan Mushkevych
"""

import inspect
import logging
import random
from model import unit_of_work_helper

from model.unit_of_work_entry import UnitOfWorkEntry
from model.single_session import SingleSessionStatistics
from system.process_context import ProcessContext
from system.collection_context import CollectionContext, COLLECTION_SINGLE_SESSION

TOTAL_ENTRIES = 101

class TestConsumer (object):
    """ empty class that should substitute MQ Flopsy Consumer. Used for testing only """
    def acknowledge(self, delivery_tag):
        pass

    def close(self):
        pass
        

class TestMessage (object):
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

    def get_group_mapping(self, timestamp, list_of_sites):
        return TestSiteMembershipDictionary(list_of_sites)

    def get_client_mapping(self, timestamp, list_of_sites):
        return TestSiteMembershipDictionary(list_of_sites)

    def get_portfolio_mapping(self, timestamp, list_of_sites):
        return TestSiteMembershipDictionary(list_of_sites)

    def get_list_of_sci(self, timestamp):
        return ['client_id_0', 'client_id_1', 'client_id_2', 'client_id_3']

    def get_calculation_components(self, timestamp, sci, sum_of_30_days):
        from workers.financial_daily_aggregator import FinancialDailyAggregator
        tuples_ch_gci = []
        tuples_sn_gci = []
        gci_to_calc = dict()

        tuples_sn_gci.append(['domain_name_4', 'client_id_0'])
        tuples_sn_gci.append(['domain_name_3', 'client_id_1'])
        tuples_sn_gci.append(['domain_name_2', 'client_id_2'])
        tuples_sn_gci.append(['domain_name_1', 'client_id_3'])
        tuples_sn_gci.append(['domain_name_0', 'client_id_4'])

        tuples_ch_gci.append(['channel_4', 'client_id_4'])
        tuples_ch_gci.append(['channel_3', 'client_id_3'])
        tuples_ch_gci.append(['channel_2', 'client_id_2'])
        tuples_ch_gci.append(['channel_1', 'client_id_1'])
        tuples_ch_gci.append(['channel_0', 'client_id_4'])

        for i in range(0, 5):
            key = 'client_id_%d' % i
            gci_to_calc[key] = { FinancialDailyAggregator.SHARE_PROTO : '0.1',
                                 FinancialDailyAggregator.SHARE_ADMIN : '0.2',
                                 FinancialDailyAggregator.SHARE_CLIENT : '0.3' }

        return {  FinancialDailyAggregator.TUPLES_DN_GCI : tuples_sn_gci,
                  FinancialDailyAggregator.TUPLES_CH_GCI : tuples_ch_gci,
                  FinancialDailyAggregator.CALC_COMPONENTS : gci_to_calc }


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


def create_unit_of_work(process_name, first_object_id, last_object_id):
    """ method is used to insert unit_of_work """
    source_collection = ProcessContext.get_source_collection(process_name)
    target_collection = ProcessContext.get_target_collection(process_name)
    logger = ProcessContext.get_logger(process_name)
    
    unit_of_work = UnitOfWorkEntry()
    unit_of_work.set_timestamp('UNIT_TEST')
    unit_of_work.set_start_id(first_object_id)
    unit_of_work.set_end_id(last_object_id)
    unit_of_work.set_source_collection(source_collection)
    unit_of_work.set_target_collection(target_collection)
    unit_of_work.set_state(UnitOfWorkEntry.STATE_REQUESTED)
    unit_of_work.set_process_name(process_name)
    unit_of_work.set_number_of_retries(0)
    
    uow_id = unit_of_work_helper.insert(logger, unit_of_work)
    return uow_id

def create_session_stats(composite_key_function, seed='RANDOM_SEED_OBJECT'):
    time_array = ['20010303102210', '20010303102212', '20010303102215', '20010303102250']
    connection = CollectionContext.get_collection(logging, COLLECTION_SINGLE_SESSION)
    random.seed(seed)
    object_ids = []
    for i in range(TOTAL_ENTRIES):
        key = composite_key_function(i, TOTAL_ENTRIES)
        session = SingleSessionStatistics()
        session.composite_key(key[0], key[1])
        session.set_session_id('session_id_' + str(i))
        session.set_ip('192.168.0.2')
        if i % 3 == 0:
            session.set_screen_res(240, 360)
        elif i % 5 == 0:
            session.set_screen_res(360, 480)
        else:
            session.set_screen_res(760, 980)

        if i % 2 == 0:
            session.set_os('Linux')
            session.set_browser('FF ' + str(i % 4))
            session.set_language('en_ca')
            session.set_country('ca')
        else:
            session.set_os('Windows')
            session.set_browser('IE ' + str(i % 9))
            session.set_language('ua_uk')
            session.set_country('eu')

        session.set_total_duration(random.randint(0, 200))
        session.set_number_of_pageviews(random.randint(1, 5))

        for index in range(random.randint(1, 4)):
            session.set_number_of_entries(index + 1)
            session.set_entry_timestamp(index, time_array[index])

        sess_id = connection.insert(session.get_document(), safe=True)
        object_ids.append(sess_id)
        
    return object_ids 

def _generate_entries(token, number, value):
    items = dict()
    for i in range(number):
        items[token + str(i)] = value
    return items


def create_site_stats(collection, composite_key_function, statistics_klass, seed='RANDOM_SEED_OBJECT'):
    connection = CollectionContext.get_collection(logging, collection)
    random.seed(seed)
    object_ids = []
    for i in range(TOTAL_ENTRIES):
        key = composite_key_function(i, TOTAL_ENTRIES)
        site_stat = statistics_klass()
        site_stat.composite_key(key[0], key[1])
        site_stat.set_number_of_visits(random.randint(1, 1000))
        site_stat.set_total_duration(random.randint(0, 100))
        
        items = _generate_entries('os_', 5, i)
        site_stat.set_os(items)
        
        items = _generate_entries('browser_', 5, i)
        site_stat.set_browsers(items)
        
        items = dict() 
        items['(320, 240)'] = 3
        items['(640, 480)'] = 5
        items['(1024, 960)'] = 7
        items['(1280, 768)'] = 9
        site_stat.set_screen_res(items)
        
        items = dict() 
        items['ca_en'] = 3
        items['ca_fr'] = 5
        items['ua_uk'] = 7
        items['us_en'] = 9
        site_stat.set_languages(items)
        
        items = dict() 
        items['ca'] = 3
        items['fr'] = 5
        items['uk'] = 7
        items['us'] = 9
        site_stat.set_countries(items)

        stat_id = connection.insert(site_stat.get_document(), safe=True)
        object_ids.append(stat_id)
                
    return object_ids 


if __name__ == '__main__':
    pass

