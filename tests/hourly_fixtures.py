__author__ = 'Bohdan Mushkevych'

import logging
from model import raw_data
from tests import base_fixtures
from system.collection_context import CollectionContext, COLLECTION_SINGLE_SESSION

# pylint: disable=C0301
EXPECTED_HOURLY_SITE_00 = {'timeperiod': '2001030311', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 1, u'en_ca': 2},
                                                               'total_duration': 254, 'number_of_visits': 3,
                                                               'number_of_pageviews': 7,
                                                               'country': {u'eu': 1, u'ca': 2},
                                                               'os': {u'Windows': 1, u'Linux': 2},
                                                               'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 4': 1}},
                           'domain_name': u'domain_name_22'}
EXPECTED_HOURLY_SITE_01 = {'timeperiod': '2001030311', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 2, u'en_ca': 1},
                                                               'total_duration': 176, 'number_of_visits': 3,
                                                               'number_of_pageviews': 4,
                                                               'country': {u'eu': 2, u'ca': 1},
                                                               'os': {u'Windows': 2, u'Linux': 1},
                                                               'browser': {u'IE 3': 1, u'FF 2': 1, u'IE 5': 1}},
                           'domain_name': u'domain_name_19'}
EXPECTED_HOURLY_SITE_02 = {'timeperiod': '2001030310', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 1, u'en_ca': 2},
                                                               'total_duration': 465, 'number_of_visits': 3,
                                                               'number_of_pageviews': 7,
                                                               'country': {u'eu': 1, u'ca': 2},
                                                               'os': {u'Windows': 1, u'Linux': 2},
                                                               'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 4': 1}},
                           'domain_name': u'domain_name_4'}
EXPECTED_HOURLY_SITE_03 = {'timeperiod': '2001030311', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 1, u'en_ca': 2},
                                                               'total_duration': 130, 'number_of_visits': 3,
                                                               'number_of_pageviews': 7,
                                                               'country': {u'eu': 1, u'ca': 2},
                                                               'os': {u'Windows': 1, u'Linux': 2},
                                                               'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 7': 1}},
                           'domain_name': u'domain_name_20'}
EXPECTED_HOURLY_SITE_04 = {'timeperiod': '2001030310', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 1, u'en_ca': 2},
                                                               'total_duration': 197, 'number_of_visits': 3,
                                                               'number_of_pageviews': 8,
                                                               'country': {u'eu': 1, u'ca': 2},
                                                               'os': {u'Windows': 1, u'Linux': 2},
                                                               'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 7': 1}},
                           'domain_name': u'domain_name_14'}
EXPECTED_HOURLY_SITE_05 = {'timeperiod': '2001030310',
                           'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
                                    'language': {u'ua_uk': 2, u'en_ca': 1}, 'total_duration': 293,
                                    'number_of_visits': 3, 'number_of_pageviews': 7, 'country': {u'eu': 2, u'ca': 1},
                                    'os': {u'Windows': 2, u'Linux': 1},
                                    'browser': {u'IE 2': 1, u'IE 0': 1, u'FF 2': 1}}, 'domain_name': u'domain_name_3'}
EXPECTED_HOURLY_SITE_06 = {'timeperiod': '2001030310',
                           'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
                                    'language': {u'ua_uk': 1, u'en_ca': 2}, 'total_duration': 163,
                                    'number_of_visits': 3, 'number_of_pageviews': 7, 'country': {u'eu': 1, u'ca': 2},
                                    'os': {u'Windows': 1, u'Linux': 2},
                                    'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 1': 1}}, 'domain_name': u'domain_name_6'}
EXPECTED_HOURLY_SITE_07 = {'timeperiod': '2001030311',
                           'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
                                    'language': {u'ua_uk': 1, u'en_ca': 2}, 'total_duration': 324,
                                    'number_of_visits': 3, 'number_of_pageviews': 6, 'country': {u'eu': 1, u'ca': 2},
                                    'os': {u'Windows': 1, u'Linux': 2},
                                    'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 1': 1}}, 'domain_name': u'domain_name_18'}
EXPECTED_HOURLY_SITE_08 = {'timeperiod': '2001030311',
                           'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
                                    'language': {u'ua_uk': 1, u'en_ca': 2}, 'total_duration': 364,
                                    'number_of_visits': 3, 'number_of_pageviews': 6, 'country': {u'eu': 1, u'ca': 2},
                                    'os': {u'Windows': 1, u'Linux': 2},
                                    'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 7': 1}}, 'domain_name': u'domain_name_26'}
EXPECTED_HOURLY_SITE_09 = {'timeperiod': '2001030311',
                           'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
                                    'language': {u'ua_uk': 2, u'en_ca': 1}, 'total_duration': 411,
                                    'number_of_visits': 3, 'number_of_pageviews': 8, 'country': {u'eu': 2, u'ca': 1},
                                    'os': {u'Windows': 2, u'Linux': 1},
                                    'browser': {u'FF 0': 1, u'IE 2': 1, u'IE 0': 1}}, 'domain_name': u'domain_name_21'}
EXPECTED_HOURLY_SITE_10 = {'timeperiod': '2001030310', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 1, u'en_ca': 2},
                                                               'total_duration': 415, 'number_of_visits': 3,
                                                               'number_of_pageviews': 5,
                                                               'country': {u'eu': 1, u'ca': 2},
                                                               'os': {u'Windows': 1, u'Linux': 2},
                                                               'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 1': 1}},
                           'domain_name': u'domain_name_12'}
EXPECTED_HOURLY_SITE_11 = {'timeperiod': '2001030310', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 2, u'en_ca': 1},
                                                               'total_duration': 227, 'number_of_visits': 3,
                                                               'number_of_pageviews': 4,
                                                               'country': {u'eu': 2, u'ca': 1},
                                                               'os': {u'Windows': 2, u'Linux': 1},
                                                               'browser': {u'FF 0': 1, u'IE 8': 1, u'IE 6': 1}},
                           'domain_name': u'domain_name_5'}
EXPECTED_HOURLY_SITE_12 = {'timeperiod': '2001030310',
                           'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
                                    'language': {u'ua_uk': 1, u'en_ca': 2}, 'total_duration': 389,
                                    'number_of_visits': 3, 'number_of_pageviews': 8, 'country': {u'eu': 1, u'ca': 2},
                                    'os': {u'Windows': 1, u'Linux': 2},
                                    'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 7': 1}}, 'domain_name': u'domain_name_8'}
EXPECTED_HOURLY_SITE_13 = {'timeperiod': '2001030310', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 2, u'en_ca': 1},
                                                               'total_duration': 242, 'number_of_visits': 3,
                                                               'number_of_pageviews': 8,
                                                               'country': {u'eu': 2, u'ca': 1},
                                                               'os': {u'Windows': 2, u'Linux': 1},
                                                               'browser': {u'FF 0': 1, u'IE 2': 1, u'IE 0': 1}},
                           'domain_name': u'domain_name_9'}
EXPECTED_HOURLY_SITE_14 = {'timeperiod': '2001030310', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 2, u'en_ca': 1},
                                                               'total_duration': 400, 'number_of_visits': 3,
                                                               'number_of_pageviews': 8,
                                                               'country': {u'eu': 2, u'ca': 1},
                                                               'os': {u'Windows': 2, u'Linux': 1},
                                                               'browser': {u'IE 2': 1, u'IE 0': 1, u'FF 2': 1}},
                           'domain_name': u'domain_name_15'}
EXPECTED_HOURLY_SITE_15 = {'timeperiod': '2001030310', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 1, u'en_ca': 2},
                                                               'total_duration': 128, 'number_of_visits': 3,
                                                               'number_of_pageviews': 6,
                                                               'country': {u'eu': 1, u'ca': 2},
                                                               'os': {u'Windows': 1, u'Linux': 2},
                                                               'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 4': 1}},
                           'domain_name': u'domain_name_10'}
EXPECTED_HOURLY_SITE_16 = {'timeperiod': '2001030310', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 2, u'en_ca': 1},
                                                               'total_duration': 320, 'number_of_visits': 3,
                                                               'number_of_pageviews': 6,
                                                               'country': {u'eu': 2, u'ca': 1},
                                                               'os': {u'Windows': 2, u'Linux': 1},
                                                               'browser': {u'IE 3': 1, u'FF 2': 1, u'IE 5': 1}},
                           'domain_name': u'domain_name_7'}
EXPECTED_HOURLY_SITE_17 = {'timeperiod': '2001030311', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 2, u'en_ca': 1},
                                                               'total_duration': 235, 'number_of_visits': 3,
                                                               'number_of_pageviews': 4,
                                                               'country': {u'eu': 2, u'ca': 1},
                                                               'os': {u'Windows': 2, u'Linux': 1},
                                                               'browser': {u'IE 2': 1, u'IE 0': 1, u'FF 2': 1}},
                           'domain_name': u'domain_name_27'}
EXPECTED_HOURLY_SITE_18 = {'timeperiod': '2001030311',
                           'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
                                    'language': {u'ua_uk': 2, u'en_ca': 1}, 'total_duration': 357,
                                    'number_of_visits': 3, 'number_of_pageviews': 5, 'country': {u'eu': 2, u'ca': 1},
                                    'os': {u'Windows': 2, u'Linux': 1},
                                    'browser': {u'IE 3': 1, u'FF 2': 1, u'IE 5': 1}}, 'domain_name': u'domain_name_31'}
EXPECTED_HOURLY_SITE_19 = {'timeperiod': '2001030310', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 1, u'en_ca': 2},
                                                               'total_duration': 217, 'number_of_visits': 3,
                                                               'number_of_pageviews': 8,
                                                               'country': {u'eu': 1, u'ca': 2},
                                                               'os': {u'Windows': 1, u'Linux': 2},
                                                               'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 1': 1}},
                           'domain_name': u'domain_name_0'}
EXPECTED_HOURLY_SITE_20 = {'timeperiod': '2001030310',
                           'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
                                    'language': {u'ua_uk': 2, u'en_ca': 1}, 'total_duration': 390,
                                    'number_of_visits': 3, 'number_of_pageviews': 6, 'country': {u'eu': 2, u'ca': 1},
                                    'os': {u'Windows': 2, u'Linux': 1},
                                    'browser': {u'FF 0': 1, u'IE 3': 1, u'IE 5': 1}}, 'domain_name': u'domain_name_13'}
EXPECTED_HOURLY_SITE_21 = {'timeperiod': '2001030311', 'stat': {'screen_resolution': {'(240, 360)': 1, '(360, 480)': 1},
                                                               'language': {u'ua_uk': 1, u'en_ca': 1},
                                                               'total_duration': 161, 'number_of_visits': 2,
                                                               'number_of_pageviews': 6,
                                                               'country': {u'eu': 1, u'ca': 1},
                                                               'os': {u'Windows': 1, u'Linux': 1},
                                                               'browser': {u'FF 0': 1, u'IE 0': 1}},
                           'domain_name': u'domain_name_33'}
EXPECTED_HOURLY_SITE_22 = {'timeperiod': '2001030311', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 1, u'en_ca': 2},
                                                               'total_duration': 349, 'number_of_visits': 3,
                                                               'number_of_pageviews': 5,
                                                               'country': {u'eu': 1, u'ca': 2},
                                                               'os': {u'Windows': 1, u'Linux': 2},
                                                               'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 1': 1}},
                           'domain_name': u'domain_name_30'}
EXPECTED_HOURLY_SITE_23 = {'timeperiod': '2001030311', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 1, u'en_ca': 2},
                                                               'total_duration': 273, 'number_of_visits': 3,
                                                               'number_of_pageviews': 5,
                                                               'country': {u'eu': 1, u'ca': 2},
                                                               'os': {u'Windows': 1, u'Linux': 2},
                                                               'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 1': 1}},
                           'domain_name': u'domain_name_24'}
EXPECTED_HOURLY_SITE_24 = {'timeperiod': '2001030310',
                           'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
                                    'language': {u'ua_uk': 2, u'en_ca': 1}, 'total_duration': 208,
                                    'number_of_visits': 3, 'number_of_pageviews': 5, 'country': {u'eu': 2, u'ca': 1},
                                    'os': {u'Windows': 2, u'Linux': 1},
                                    'browser': {u'FF 2': 1, u'IE 6': 1, u'IE 8': 1}}, 'domain_name': u'domain_name_11'}
EXPECTED_HOURLY_SITE_25 = {'timeperiod': '2001030310',
                           'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
                                    'language': {u'ua_uk': 1, u'en_ca': 2}, 'total_duration': 399,
                                    'number_of_visits': 3, 'number_of_pageviews': 6, 'country': {u'eu': 1, u'ca': 2},
                                    'os': {u'Windows': 1, u'Linux': 2},
                                    'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 4': 1}}, 'domain_name': u'domain_name_16'}
EXPECTED_HOURLY_SITE_26 = {'timeperiod': '2001030311', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 1, u'en_ca': 2},
                                                               'total_duration': 400, 'number_of_visits': 3,
                                                               'number_of_pageviews': 7,
                                                               'country': {u'eu': 1, u'ca': 2},
                                                               'os': {u'Windows': 1, u'Linux': 2},
                                                               'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 7': 1}},
                           'domain_name': u'domain_name_32'}
EXPECTED_HOURLY_SITE_27 = {'timeperiod': '2001030310',
                           'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
                                    'language': {u'ua_uk': 2, u'en_ca': 1}, 'total_duration': 374,
                                    'number_of_visits': 3, 'number_of_pageviews': 5, 'country': {u'eu': 2, u'ca': 1},
                                    'os': {u'Windows': 2, u'Linux': 1},
                                    'browser': {u'FF 0': 1, u'IE 3': 1, u'IE 5': 1}}, 'domain_name': u'domain_name_1'}
EXPECTED_HOURLY_SITE_28 = {'timeperiod': '2001030311',
                           'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
                                    'language': {u'ua_uk': 1, u'en_ca': 2}, 'total_duration': 434,
                                    'number_of_visits': 3, 'number_of_pageviews': 5, 'country': {u'eu': 1, u'ca': 2},
                                    'os': {u'Windows': 1, u'Linux': 2},
                                    'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 4': 1}}, 'domain_name': u'domain_name_28'}
EXPECTED_HOURLY_SITE_29 = {'timeperiod': '2001030311', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 2, u'en_ca': 1},
                                                               'total_duration': 170, 'number_of_visits': 3,
                                                               'number_of_pageviews': 4,
                                                               'country': {u'eu': 2, u'ca': 1},
                                                               'os': {u'Windows': 2, u'Linux': 1},
                                                               'browser': {u'FF 0': 1, u'IE 8': 1, u'IE 6': 1}},
                           'domain_name': u'domain_name_17'}
EXPECTED_HOURLY_SITE_30 = {'timeperiod': '2001030311', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 2, u'en_ca': 1},
                                                               'total_duration': 378, 'number_of_visits': 3,
                                                               'number_of_pageviews': 6,
                                                               'country': {u'eu': 2, u'ca': 1},
                                                               'os': {u'Windows': 2, u'Linux': 1},
                                                               'browser': {u'FF 0': 1, u'IE 3': 1, u'IE 5': 1}},
                           'domain_name': u'domain_name_25'}
EXPECTED_HOURLY_SITE_31 = {'timeperiod': '2001030310', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 1, u'en_ca': 2},
                                                               'total_duration': 423, 'number_of_visits': 3,
                                                               'number_of_pageviews': 7,
                                                               'country': {u'eu': 1, u'ca': 2},
                                                               'os': {u'Windows': 1, u'Linux': 2},
                                                               'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 7': 1}},
                           'domain_name': u'domain_name_2'}
EXPECTED_HOURLY_SITE_32 = {'timeperiod': '2001030311',
                           'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
                                    'language': {u'ua_uk': 2, u'en_ca': 1}, 'total_duration': 180,
                                    'number_of_visits': 3, 'number_of_pageviews': 4, 'country': {u'eu': 2, u'ca': 1},
                                    'os': {u'Windows': 2, u'Linux': 1},
                                    'browser': {u'FF 2': 1, u'IE 6': 1, u'IE 8': 1}}, 'domain_name': u'domain_name_23'}
EXPECTED_HOURLY_SITE_33 = {'timeperiod': '2001030311', 'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1},
                                                               'language': {u'ua_uk': 2, u'en_ca': 1},
                                                               'total_duration': 268, 'number_of_visits': 3,
                                                               'number_of_pageviews': 6,
                                                               'country': {u'eu': 2, u'ca': 1},
                                                               'os': {u'Windows': 2, u'Linux': 1},
                                                               'browser': {u'FF 0': 1, u'IE 8': 1, u'IE 6': 1}},
                           'domain_name': u'domain_name_29'}
# pylint: enable=C0301


def generate_session_composite_key(index, total):
    h1 = '20010303101010'
    h2 = '20010303111111'

    if index <= total / 2:
        return 'domain_name_%s' % str(index // 3), h1
    else:
        return 'domain_name_%s' % str(index // 3), h2


def clean_session_entries():
    connection = CollectionContext.get_collection(logging, COLLECTION_SINGLE_SESSION)
    for i in range(base_fixtures.TOTAL_ENTRIES):
        key = generate_session_composite_key(i, base_fixtures.TOTAL_ENTRIES)
        connection.remove({
            raw_data.KEY: key[0],
            raw_data.TIMEPERIOD: key[1],
            raw_data.FAMILY_USER_PROFILE + '.' + raw_data.SESSION_ID: 'session_id_%s' % str(i)})


def generated_session_entries():
    return base_fixtures.create_session_stats(generate_session_composite_key)


if __name__ == '__main__':
    pass