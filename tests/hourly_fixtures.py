__author__ = 'Bohdan Mushkevych'

from tests import base_fixtures
from db.model import raw_data
from constants import COLLECTION_SINGLE_SESSION
from synergy.db.manager import ds_manager
from synergy.conf.process_context import ProcessContext
from tests.ut_context import PROCESS_UNIT_TEST

# pylint: disable=C0301
EXPECTED_HOURLY_SITE_00 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 1, u'en_ca': 2},
             'total_duration': 360, 'number_of_visits': 3, 'number_of_pageviews': 11, 'country': {u'eu': 1, u'ca': 2},
             'os': {u'Windows': 1, u'Linux': 2}, 'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 4': 1}},
    'timeperiod': '2001030311', 'domain_name': u'domain_name_22'}

EXPECTED_HOURLY_SITE_01 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 2, u'en_ca': 1},
             'total_duration': 411, 'number_of_visits': 3, 'number_of_pageviews': 9, 'country': {u'eu': 2, u'ca': 1},
             'os': {u'Windows': 2, u'Linux': 1}, 'browser': {u'IE 3': 1, u'FF 2': 1, u'IE 5': 1}},
    'timeperiod': '2001030311', 'domain_name': u'domain_name_19'}

EXPECTED_HOURLY_SITE_02 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 1, u'en_ca': 2},
             'total_duration': 380, 'number_of_visits': 3, 'number_of_pageviews': 9, 'country': {u'eu': 1, u'ca': 2},
             'os': {u'Windows': 1, u'Linux': 2}, 'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 4': 1}},
    'timeperiod': '2001030310', 'domain_name': u'domain_name_4'}

EXPECTED_HOURLY_SITE_03 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 1, u'en_ca': 2},
             'total_duration': 336, 'number_of_visits': 3, 'number_of_pageviews': 7, 'country': {u'eu': 1, u'ca': 2},
             'os': {u'Windows': 1, u'Linux': 2}, 'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 7': 1}},
    'timeperiod': '2001030311', 'domain_name': u'domain_name_20'}

EXPECTED_HOURLY_SITE_04 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 1, u'en_ca': 2},
             'total_duration': 363, 'number_of_visits': 3, 'number_of_pageviews': 12, 'country': {u'eu': 1, u'ca': 2},
             'os': {u'Windows': 1, u'Linux': 2}, 'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 7': 1}},
    'timeperiod': '2001030310', 'domain_name': u'domain_name_14'}

EXPECTED_HOURLY_SITE_05 = {
    'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
             'language': {u'ua_uk': 2, u'en_ca': 1}, 'total_duration': 352,
             'number_of_visits': 3, 'number_of_pageviews': 7, 'country': {u'eu': 2, u'ca': 1},
             'os': {u'Windows': 2, u'Linux': 1},
             'browser': {u'IE 2': 1, u'IE 0': 1, u'FF 2': 1}}, 'timeperiod': '2001030310',
    'domain_name': u'domain_name_3'}

EXPECTED_HOURLY_SITE_06 = {
    'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
             'language': {u'ua_uk': 1, u'en_ca': 2}, 'total_duration': 409,
             'number_of_visits': 3, 'number_of_pageviews': 7, 'country': {u'eu': 1, u'ca': 2},
             'os': {u'Windows': 1, u'Linux': 2},
             'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 1': 1}}, 'timeperiod': '2001030310',
    'domain_name': u'domain_name_6'}

EXPECTED_HOURLY_SITE_07 = {
    'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
             'language': {u'ua_uk': 1, u'en_ca': 2}, 'total_duration': 281,
             'number_of_visits': 3, 'number_of_pageviews': 10, 'country': {u'eu': 1, u'ca': 2},
             'os': {u'Windows': 1, u'Linux': 2},
             'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 1': 1}}, 'timeperiod': '2001030311',
    'domain_name': u'domain_name_18'}

EXPECTED_HOURLY_SITE_08 = {
    'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
             'language': {u'ua_uk': 1, u'en_ca': 2}, 'total_duration': 367,
             'number_of_visits': 3, 'number_of_pageviews': 8, 'country': {u'eu': 1, u'ca': 2},
             'os': {u'Windows': 1, u'Linux': 2},
             'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 7': 1}}, 'timeperiod': '2001030311',
    'domain_name': u'domain_name_26'}

EXPECTED_HOURLY_SITE_09 = {
    'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
             'language': {u'ua_uk': 2, u'en_ca': 1}, 'total_duration': 447,
             'number_of_visits': 3, 'number_of_pageviews': 7, 'country': {u'eu': 2, u'ca': 1},
             'os': {u'Windows': 2, u'Linux': 1},
             'browser': {u'FF 0': 1, u'IE 2': 1, u'IE 0': 1}}, 'timeperiod': '2001030311',
    'domain_name': u'domain_name_21'}

EXPECTED_HOURLY_SITE_10 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 1, u'en_ca': 2},
             'total_duration': 445, 'number_of_visits': 3, 'number_of_pageviews': 8, 'country': {u'eu': 1, u'ca': 2},
             'os': {u'Windows': 1, u'Linux': 2}, 'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 1': 1}},
    'timeperiod': '2001030310', 'domain_name': u'domain_name_12'}

EXPECTED_HOURLY_SITE_11 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 2, u'en_ca': 1},
             'total_duration': 281, 'number_of_visits': 3, 'number_of_pageviews': 11, 'country': {u'eu': 2, u'ca': 1},
             'os': {u'Windows': 2, u'Linux': 1}, 'browser': {u'FF 0': 1, u'IE 8': 1, u'IE 6': 1}},
    'timeperiod': '2001030310', 'domain_name': u'domain_name_5'}

EXPECTED_HOURLY_SITE_12 = {
    'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
             'language': {u'ua_uk': 1, u'en_ca': 2}, 'total_duration': 387,
             'number_of_visits': 3, 'number_of_pageviews': 12, 'country': {u'eu': 1, u'ca': 2},
             'os': {u'Windows': 1, u'Linux': 2},
             'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 7': 1}}, 'timeperiod': '2001030310',
    'domain_name': u'domain_name_8'}

EXPECTED_HOURLY_SITE_13 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 2, u'en_ca': 1},
             'total_duration': 300, 'number_of_visits': 3, 'number_of_pageviews': 10, 'country': {u'eu': 2, u'ca': 1},
             'os': {u'Windows': 2, u'Linux': 1}, 'browser': {u'FF 0': 1, u'IE 2': 1, u'IE 0': 1}},
    'timeperiod': '2001030310', 'domain_name': u'domain_name_9'}

EXPECTED_HOURLY_SITE_14 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 2, u'en_ca': 1},
             'total_duration': 314, 'number_of_visits': 3, 'number_of_pageviews': 12, 'country': {u'eu': 2, u'ca': 1},
             'os': {u'Windows': 2, u'Linux': 1}, 'browser': {u'IE 2': 1, u'IE 0': 1, u'FF 2': 1}},
    'timeperiod': '2001030310', 'domain_name': u'domain_name_15'}

EXPECTED_HOURLY_SITE_15 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 1, u'en_ca': 2},
             'total_duration': 65, 'number_of_visits': 3, 'number_of_pageviews': 12, 'country': {u'eu': 1, u'ca': 2},
             'os': {u'Windows': 1, u'Linux': 2}, 'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 4': 1}},
    'timeperiod': '2001030310', 'domain_name': u'domain_name_10'}

EXPECTED_HOURLY_SITE_16 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 2, u'en_ca': 1},
             'total_duration': 120, 'number_of_visits': 3, 'number_of_pageviews': 13, 'country': {u'eu': 2, u'ca': 1},
             'os': {u'Windows': 2, u'Linux': 1}, 'browser': {u'IE 3': 1, u'FF 2': 1, u'IE 5': 1}},
    'timeperiod': '2001030310', 'domain_name': u'domain_name_7'}

EXPECTED_HOURLY_SITE_17 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 2, u'en_ca': 1},
             'total_duration': 479, 'number_of_visits': 3, 'number_of_pageviews': 9, 'country': {u'eu': 2, u'ca': 1},
             'os': {u'Windows': 2, u'Linux': 1}, 'browser': {u'IE 2': 1, u'IE 0': 1, u'FF 2': 1}},
    'timeperiod': '2001030311', 'domain_name': u'domain_name_27'}

EXPECTED_HOURLY_SITE_18 = {
    'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
             'language': {u'ua_uk': 2, u'en_ca': 1}, 'total_duration': 437,
             'number_of_visits': 3, 'number_of_pageviews': 10, 'country': {u'eu': 2, u'ca': 1},
             'os': {u'Windows': 2, u'Linux': 1},
             'browser': {u'IE 3': 1, u'FF 2': 1, u'IE 5': 1}}, 'timeperiod': '2001030311',
    'domain_name': u'domain_name_31'}

EXPECTED_HOURLY_SITE_19 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 1, u'en_ca': 2},
             'total_duration': 379, 'number_of_visits': 3, 'number_of_pageviews': 10, 'country': {u'eu': 1, u'ca': 2},
             'os': {u'Windows': 1, u'Linux': 2}, 'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 1': 1}},
    'timeperiod': '2001030310', 'domain_name': u'domain_name_0'}

EXPECTED_HOURLY_SITE_20 = {
    'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
             'language': {u'ua_uk': 2, u'en_ca': 1}, 'total_duration': 348,
             'number_of_visits': 3, 'number_of_pageviews': 8, 'country': {u'eu': 2, u'ca': 1},
             'os': {u'Windows': 2, u'Linux': 1},
             'browser': {u'FF 0': 1, u'IE 3': 1, u'IE 5': 1}}, 'timeperiod': '2001030310',
    'domain_name': u'domain_name_13'}

EXPECTED_HOURLY_SITE_21 = {
    'stat': {'screen_resolution': {'(240, 360)': 1, '(360, 480)': 1}, 'language': {u'ua_uk': 1, u'en_ca': 1},
             'total_duration': 220, 'number_of_visits': 2, 'number_of_pageviews': 6, 'country': {u'eu': 1, u'ca': 1},
             'os': {u'Windows': 1, u'Linux': 1}, 'browser': {u'FF 0': 1, u'IE 0': 1}}, 'timeperiod': '2001030311',
    'domain_name': u'domain_name_33'}

EXPECTED_HOURLY_SITE_22 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 1, u'en_ca': 2},
             'total_duration': 428, 'number_of_visits': 3, 'number_of_pageviews': 7, 'country': {u'eu': 1, u'ca': 2},
             'os': {u'Windows': 1, u'Linux': 2}, 'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 1': 1}},
    'timeperiod': '2001030311', 'domain_name': u'domain_name_30'}

EXPECTED_HOURLY_SITE_23 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 1, u'en_ca': 2},
             'total_duration': 95, 'number_of_visits': 3, 'number_of_pageviews': 10, 'country': {u'eu': 1, u'ca': 2},
             'os': {u'Windows': 1, u'Linux': 2}, 'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 1': 1}},
    'timeperiod': '2001030311', 'domain_name': u'domain_name_24'}

EXPECTED_HOURLY_SITE_24 = {
    'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
             'language': {u'ua_uk': 2, u'en_ca': 1}, 'total_duration': 181,
             'number_of_visits': 3, 'number_of_pageviews': 10, 'country': {u'eu': 2, u'ca': 1},
             'os': {u'Windows': 2, u'Linux': 1},
             'browser': {u'FF 2': 1, u'IE 6': 1, u'IE 8': 1}}, 'timeperiod': '2001030310',
    'domain_name': u'domain_name_11'}

EXPECTED_HOURLY_SITE_25 = {
    'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
             'language': {u'ua_uk': 1, u'en_ca': 2}, 'total_duration': 311,
             'number_of_visits': 3, 'number_of_pageviews': 7, 'country': {u'eu': 1, u'ca': 2},
             'os': {u'Windows': 1, u'Linux': 2},
             'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 4': 1}}, 'timeperiod': '2001030310',
    'domain_name': u'domain_name_16'}

EXPECTED_HOURLY_SITE_26 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 1, u'en_ca': 2},
             'total_duration': 282, 'number_of_visits': 3, 'number_of_pageviews': 11, 'country': {u'eu': 1, u'ca': 2},
             'os': {u'Windows': 1, u'Linux': 2}, 'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 7': 1}},
    'timeperiod': '2001030311', 'domain_name': u'domain_name_32'}

EXPECTED_HOURLY_SITE_27 = {
    'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
             'language': {u'ua_uk': 2, u'en_ca': 1}, 'total_duration': 264,
             'number_of_visits': 3, 'number_of_pageviews': 6, 'country': {u'eu': 2, u'ca': 1},
             'os': {u'Windows': 2, u'Linux': 1},
             'browser': {u'FF 0': 1, u'IE 3': 1, u'IE 5': 1}}, 'timeperiod': '2001030310',
    'domain_name': u'domain_name_1'}

EXPECTED_HOURLY_SITE_28 = {
    'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
             'language': {u'ua_uk': 1, u'en_ca': 2}, 'total_duration': 77, 'number_of_visits': 3,
             'number_of_pageviews': 9, 'country': {u'eu': 1, u'ca': 2},
             'os': {u'Windows': 1, u'Linux': 2},
             'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 4': 1}}, 'timeperiod': '2001030311',
    'domain_name': u'domain_name_28'}

EXPECTED_HOURLY_SITE_29 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 2, u'en_ca': 1},
             'total_duration': 21, 'number_of_visits': 3, 'number_of_pageviews': 4, 'country': {u'eu': 2, u'ca': 1},
             'os': {u'Windows': 2, u'Linux': 1}, 'browser': {u'FF 0': 1, u'IE 8': 1, u'IE 6': 1}},
    'timeperiod': '2001030311', 'domain_name': u'domain_name_17'}

EXPECTED_HOURLY_SITE_30 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 2, u'en_ca': 1},
             'total_duration': 420, 'number_of_visits': 3, 'number_of_pageviews': 10, 'country': {u'eu': 2, u'ca': 1},
             'os': {u'Windows': 2, u'Linux': 1}, 'browser': {u'FF 0': 1, u'IE 3': 1, u'IE 5': 1}},
    'timeperiod': '2001030311', 'domain_name': u'domain_name_25'}

EXPECTED_HOURLY_SITE_31 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 1, u'en_ca': 2},
             'total_duration': 181, 'number_of_visits': 3, 'number_of_pageviews': 7, 'country': {u'eu': 1, u'ca': 2},
             'os': {u'Windows': 1, u'Linux': 2}, 'browser': {u'FF 0': 1, u'FF 2': 1, u'IE 7': 1}},
    'timeperiod': '2001030310', 'domain_name': u'domain_name_2'}

EXPECTED_HOURLY_SITE_32 = {
    'stat': {'screen_resolution': {'(760, 980)': 1, '(240, 360)': 1, '(360, 480)': 1},
             'language': {u'ua_uk': 2, u'en_ca': 1}, 'total_duration': 237,
             'number_of_visits': 3, 'number_of_pageviews': 10, 'country': {u'eu': 2, u'ca': 1},
             'os': {u'Windows': 2, u'Linux': 1},
             'browser': {u'FF 2': 1, u'IE 6': 1, u'IE 8': 1}}, 'timeperiod': '2001030311',
    'domain_name': u'domain_name_23'}

EXPECTED_HOURLY_SITE_33 = {
    'stat': {'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'language': {u'ua_uk': 2, u'en_ca': 1},
             'total_duration': 227, 'number_of_visits': 3, 'number_of_pageviews': 9, 'country': {u'eu': 2, u'ca': 1},
             'os': {u'Windows': 2, u'Linux': 1}, 'browser': {u'FF 0': 1, u'IE 8': 1, u'IE 6': 1}},
    'timeperiod': '2001030311', 'domain_name': u'domain_name_29'}
# pylint: enable=C0301


def generate_session_composite_key(index, total):
    h1 = '20010303101010'
    h2 = '20010303111111'

    if index <= total / 2:
        return 'domain_name_%s' % str(index // 3), h1
    else:
        return 'domain_name_%s' % str(index // 3), h2


def clean_session_entries():
    logger = ProcessContext.get_logger(PROCESS_UNIT_TEST)
    ds = ds_manager.ds_factory(logger)
    connection = ds.connection(COLLECTION_SINGLE_SESSION)
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