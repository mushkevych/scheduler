__author__ = 'Bohdan Mushkevych'

from synergy.system.time_qualifier import *

from tests import base_fixtures
from db.dao.single_session_dao import SingleSessionDao
from constants import COLLECTION_SITE_HOURLY
from synergy.system.system_logger import get_logger
from tests.ut_context import PROCESS_UNIT_TEST

# pylint: disable=C0301
EXPECTED_SITE_HOURLY_00 = {'timeperiod': '2001030311', 'stat': {'language': {'en_ca': 1, 'ua_uk': 1}, 'total_duration': 5227367430, 'screen_resolution': {'(360, 480)': 1, '(240, 360)': 1}, 'country': {'ca': 1, 'eu': 1}, 'number_of_visits': 2, 'os': {'Linux': 1, 'Windows': 1}, 'number_of_pageviews': 3740956482, 'browser': {'IE 0': 1, 'FF 0': 1}}, 'domain': 'domain_name_33'}
EXPECTED_SITE_HOURLY_01 = {'timeperiod': '2001030310', 'stat': {'language': {'en_ca': 1, 'ua_uk': 2}, 'total_duration': 7653408865, 'screen_resolution': {'(360, 480)': 1, '(760, 980)': 1, '(240, 360)': 1}, 'country': {'ca': 1, 'eu': 2}, 'number_of_visits': 3, 'os': {'Linux': 1, 'Windows': 2}, 'number_of_pageviews': 3134891973, 'browser': {'IE 5': 1, 'FF 0': 1, 'IE 3': 1}}, 'domain': 'domain_name_13'}
EXPECTED_SITE_HOURLY_02 = {'timeperiod': '2001030311', 'stat': {'language': {'en_ca': 2, 'ua_uk': 1}, 'total_duration': 1779834629, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'eu': 1, 'ca': 2}, 'number_of_visits': 3, 'os': {'Linux': 2, 'Windows': 1}, 'number_of_pageviews': 2629191548, 'browser': {'IE 4': 1, 'FF 0': 1, 'FF 2': 1}}, 'domain': 'domain_name_22'}
EXPECTED_SITE_HOURLY_03 = {'timeperiod': '2001030311', 'stat': {'language': {'en_ca': 2, 'ua_uk': 1}, 'total_duration': 7649003925, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'eu': 1, 'ca': 2}, 'number_of_visits': 3, 'os': {'Linux': 2, 'Windows': 1}, 'number_of_pageviews': 9244195247, 'browser': {'IE 7': 1, 'FF 0': 1, 'FF 2': 1}}, 'domain': 'domain_name_20'}
EXPECTED_SITE_HOURLY_04 = {'timeperiod': '2001030311', 'stat': {'language': {'en_ca': 2, 'ua_uk': 1}, 'total_duration': 4162966124, 'screen_resolution': {'(360, 480)': 1, '(760, 980)': 1, '(240, 360)': 1}, 'country': {'eu': 1, 'ca': 2}, 'number_of_visits': 3, 'os': {'Linux': 2, 'Windows': 1}, 'number_of_pageviews': 8134666243, 'browser': {'IE 1': 1, 'FF 0': 1, 'FF 2': 1}}, 'domain': 'domain_name_18'}
EXPECTED_SITE_HOURLY_05 = {'timeperiod': '2001030310', 'stat': {'language': {'en_ca': 2, 'ua_uk': 1}, 'total_duration': 4991848997, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'eu': 1, 'ca': 2}, 'number_of_visits': 3, 'os': {'Linux': 2, 'Windows': 1}, 'number_of_pageviews': 10192947589, 'browser': {'IE 1': 1, 'FF 0': 1, 'FF 2': 1}}, 'domain': 'domain_name_12'}
EXPECTED_SITE_HOURLY_06 = {'timeperiod': '2001030311', 'stat': {'language': {'en_ca': 2, 'ua_uk': 1}, 'total_duration': 7881804295, 'screen_resolution': {'(360, 480)': 1, '(760, 980)': 1, '(240, 360)': 1}, 'country': {'eu': 1, 'ca': 2}, 'number_of_visits': 3, 'os': {'Linux': 2, 'Windows': 1}, 'number_of_pageviews': 8030672493, 'browser': {'IE 7': 1, 'FF 0': 1, 'FF 2': 1}}, 'domain': 'domain_name_26'}
EXPECTED_SITE_HOURLY_07 = {'timeperiod': '2001030311', 'stat': {'language': {'en_ca': 1, 'ua_uk': 2}, 'total_duration': 6190241731, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'ca': 1, 'eu': 2}, 'number_of_visits': 3, 'os': {'Linux': 1, 'Windows': 2}, 'number_of_pageviews': 5163626943, 'browser': {'IE 8': 1, 'FF 0': 1, 'IE 6': 1}}, 'domain': 'domain_name_17'}
EXPECTED_SITE_HOURLY_08 = {'timeperiod': '2001030310', 'stat': {'language': {'en_ca': 2, 'ua_uk': 1}, 'total_duration': 3717053201, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'eu': 1, 'ca': 2}, 'number_of_visits': 3, 'os': {'Linux': 2, 'Windows': 1}, 'number_of_pageviews': 9973077489, 'browser': {'IE 1': 1, 'FF 0': 1, 'FF 2': 1}}, 'domain': 'domain_name_0'}
EXPECTED_SITE_HOURLY_09 = {'timeperiod': '2001030310', 'stat': {'language': {'en_ca': 2, 'ua_uk': 1}, 'total_duration': 5320140741, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'eu': 1, 'ca': 2}, 'number_of_visits': 3, 'os': {'Linux': 2, 'Windows': 1}, 'number_of_pageviews': 6246512044, 'browser': {'IE 4': 1, 'FF 0': 1, 'FF 2': 1}}, 'domain': 'domain_name_10'}
EXPECTED_SITE_HOURLY_10 = {'timeperiod': '2001030310', 'stat': {'language': {'en_ca': 1, 'ua_uk': 2}, 'total_duration': 6061893719, 'screen_resolution': {'(360, 480)': 1, '(760, 980)': 1, '(240, 360)': 1}, 'country': {'ca': 1, 'eu': 2}, 'number_of_visits': 3, 'os': {'Linux': 1, 'Windows': 2}, 'number_of_pageviews': 5563196202, 'browser': {'IE 5': 1, 'FF 0': 1, 'IE 3': 1}}, 'domain': 'domain_name_1'}
EXPECTED_SITE_HOURLY_11 = {'timeperiod': '2001030310', 'stat': {'language': {'en_ca': 2, 'ua_uk': 1}, 'total_duration': 5805107426, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'eu': 1, 'ca': 2}, 'number_of_visits': 3, 'os': {'Linux': 2, 'Windows': 1}, 'number_of_pageviews': 4129079875, 'browser': {'IE 7': 1, 'FF 0': 1, 'FF 2': 1}}, 'domain': 'domain_name_2'}
EXPECTED_SITE_HOURLY_12 = {'timeperiod': '2001030311', 'stat': {'language': {'en_ca': 1, 'ua_uk': 2}, 'total_duration': 10592739677, 'screen_resolution': {'(360, 480)': 1, '(760, 980)': 1, '(240, 360)': 1}, 'country': {'ca': 1, 'eu': 2}, 'number_of_visits': 3, 'os': {'Linux': 1, 'Windows': 2}, 'number_of_pageviews': 8790964871, 'browser': {'IE 0': 1, 'FF 0': 1, 'IE 2': 1}}, 'domain': 'domain_name_21'}
EXPECTED_SITE_HOURLY_13 = {'timeperiod': '2001030310', 'stat': {'language': {'en_ca': 1, 'ua_uk': 2}, 'total_duration': 7663563682, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'ca': 1, 'eu': 2}, 'number_of_visits': 3, 'os': {'Linux': 1, 'Windows': 2}, 'number_of_pageviews': 10506065037, 'browser': {'IE 8': 1, 'FF 0': 1, 'IE 6': 1}}, 'domain': 'domain_name_5'}
EXPECTED_SITE_HOURLY_14 = {'timeperiod': '2001030311', 'stat': {'language': {'en_ca': 1, 'ua_uk': 2}, 'total_duration': 6196172332, 'screen_resolution': {'(360, 480)': 1, '(760, 980)': 1, '(240, 360)': 1}, 'country': {'ca': 1, 'eu': 2}, 'number_of_visits': 3, 'os': {'Linux': 1, 'Windows': 2}, 'number_of_pageviews': 8984642011, 'browser': {'IE 5': 1, 'FF 2': 1, 'IE 3': 1}}, 'domain': 'domain_name_31'}
EXPECTED_SITE_HOURLY_15 = {'timeperiod': '2001030310', 'stat': {'language': {'en_ca': 2, 'ua_uk': 1}, 'total_duration': 9037230910, 'screen_resolution': {'(360, 480)': 1, '(760, 980)': 1, '(240, 360)': 1}, 'country': {'eu': 1, 'ca': 2}, 'number_of_visits': 3, 'os': {'Linux': 2, 'Windows': 1}, 'number_of_pageviews': 4590716110, 'browser': {'IE 1': 1, 'FF 0': 1, 'FF 2': 1}}, 'domain': 'domain_name_6'}
EXPECTED_SITE_HOURLY_16 = {'timeperiod': '2001030311', 'stat': {'language': {'en_ca': 1, 'ua_uk': 2}, 'total_duration': 7280556244, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'ca': 1, 'eu': 2}, 'number_of_visits': 3, 'os': {'Linux': 1, 'Windows': 2}, 'number_of_pageviews': 5178732710, 'browser': {'IE 8': 1, 'FF 0': 1, 'IE 6': 1}}, 'domain': 'domain_name_29'}
EXPECTED_SITE_HOURLY_17 = {'timeperiod': '2001030310', 'stat': {'language': {'en_ca': 2, 'ua_uk': 1}, 'total_duration': 6615259441, 'screen_resolution': {'(360, 480)': 1, '(760, 980)': 1, '(240, 360)': 1}, 'country': {'eu': 1, 'ca': 2}, 'number_of_visits': 3, 'os': {'Linux': 2, 'Windows': 1}, 'number_of_pageviews': 6533963672, 'browser': {'IE 4': 1, 'FF 0': 1, 'FF 2': 1}}, 'domain': 'domain_name_16'}
EXPECTED_SITE_HOURLY_18 = {'timeperiod': '2001030310', 'stat': {'language': {'en_ca': 2, 'ua_uk': 1}, 'total_duration': 7739804249, 'screen_resolution': {'(360, 480)': 1, '(760, 980)': 1, '(240, 360)': 1}, 'country': {'eu': 1, 'ca': 2}, 'number_of_visits': 3, 'os': {'Linux': 2, 'Windows': 1}, 'number_of_pageviews': 8716926493, 'browser': {'IE 7': 1, 'FF 0': 1, 'FF 2': 1}}, 'domain': 'domain_name_8'}
EXPECTED_SITE_HOURLY_19 = {'timeperiod': '2001030311', 'stat': {'language': {'en_ca': 1, 'ua_uk': 2}, 'total_duration': 7317507590, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'ca': 1, 'eu': 2}, 'number_of_visits': 3, 'os': {'Linux': 1, 'Windows': 2}, 'number_of_pageviews': 3950164562, 'browser': {'IE 0': 1, 'IE 2': 1, 'FF 2': 1}}, 'domain': 'domain_name_27'}
EXPECTED_SITE_HOURLY_20 = {'timeperiod': '2001030310', 'stat': {'language': {'en_ca': 2, 'ua_uk': 1}, 'total_duration': 5477178958, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'eu': 1, 'ca': 2}, 'number_of_visits': 3, 'os': {'Linux': 2, 'Windows': 1}, 'number_of_pageviews': 6857326739, 'browser': {'IE 4': 1, 'FF 0': 1, 'FF 2': 1}}, 'domain': 'domain_name_4'}
EXPECTED_SITE_HOURLY_21 = {'timeperiod': '2001030311', 'stat': {'language': {'en_ca': 2, 'ua_uk': 1}, 'total_duration': 7375105439, 'screen_resolution': {'(360, 480)': 1, '(760, 980)': 1, '(240, 360)': 1}, 'country': {'eu': 1, 'ca': 2}, 'number_of_visits': 3, 'os': {'Linux': 2, 'Windows': 1}, 'number_of_pageviews': 8573737755, 'browser': {'IE 4': 1, 'FF 0': 1, 'FF 2': 1}}, 'domain': 'domain_name_28'}
EXPECTED_SITE_HOURLY_22 = {'timeperiod': '2001030311', 'stat': {'language': {'en_ca': 1, 'ua_uk': 2}, 'total_duration': 6923904887, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'ca': 1, 'eu': 2}, 'number_of_visits': 3, 'os': {'Linux': 1, 'Windows': 2}, 'number_of_pageviews': 7294348571, 'browser': {'IE 5': 1, 'FF 2': 1, 'IE 3': 1}}, 'domain': 'domain_name_19'}
EXPECTED_SITE_HOURLY_23 = {'timeperiod': '2001030311', 'stat': {'language': {'en_ca': 1, 'ua_uk': 2}, 'total_duration': 4538162987, 'screen_resolution': {'(360, 480)': 1, '(760, 980)': 1, '(240, 360)': 1}, 'country': {'ca': 1, 'eu': 2}, 'number_of_visits': 3, 'os': {'Linux': 1, 'Windows': 2}, 'number_of_pageviews': 6366384870, 'browser': {'IE 8': 1, 'IE 6': 1, 'FF 2': 1}}, 'domain': 'domain_name_23'}
EXPECTED_SITE_HOURLY_24 = {'timeperiod': '2001030310', 'stat': {'language': {'en_ca': 1, 'ua_uk': 2}, 'total_duration': 6988363360, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'ca': 1, 'eu': 2}, 'number_of_visits': 3, 'os': {'Linux': 1, 'Windows': 2}, 'number_of_pageviews': 10204673803, 'browser': {'IE 0': 1, 'FF 0': 1, 'IE 2': 1}}, 'domain': 'domain_name_9'}
EXPECTED_SITE_HOURLY_25 = {'timeperiod': '2001030311', 'stat': {'language': {'en_ca': 2, 'ua_uk': 1}, 'total_duration': 7403620061, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'eu': 1, 'ca': 2}, 'number_of_visits': 3, 'os': {'Linux': 2, 'Windows': 1}, 'number_of_pageviews': 10398292477, 'browser': {'IE 7': 1, 'FF 0': 1, 'FF 2': 1}}, 'domain': 'domain_name_32'}
EXPECTED_SITE_HOURLY_26 = {'timeperiod': '2001030310', 'stat': {'language': {'en_ca': 2, 'ua_uk': 1}, 'total_duration': 3842774360, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'eu': 1, 'ca': 2}, 'number_of_visits': 3, 'os': {'Linux': 2, 'Windows': 1}, 'number_of_pageviews': 5736810688, 'browser': {'IE 7': 1, 'FF 0': 1, 'FF 2': 1}}, 'domain': 'domain_name_14'}
EXPECTED_SITE_HOURLY_27 = {'timeperiod': '2001030310', 'stat': {'language': {'en_ca': 1, 'ua_uk': 2}, 'total_duration': 6403650623, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'ca': 1, 'eu': 2}, 'number_of_visits': 3, 'os': {'Linux': 1, 'Windows': 2}, 'number_of_pageviews': 4902048151, 'browser': {'IE 5': 1, 'FF 2': 1, 'IE 3': 1}}, 'domain': 'domain_name_7'}
EXPECTED_SITE_HOURLY_28 = {'timeperiod': '2001030311', 'stat': {'language': {'en_ca': 1, 'ua_uk': 2}, 'total_duration': 7224452249, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'ca': 1, 'eu': 2}, 'number_of_visits': 3, 'os': {'Linux': 1, 'Windows': 2}, 'number_of_pageviews': 6642109714, 'browser': {'IE 5': 1, 'FF 0': 1, 'IE 3': 1}}, 'domain': 'domain_name_25'}
EXPECTED_SITE_HOURLY_29 = {'timeperiod': '2001030311', 'stat': {'language': {'en_ca': 2, 'ua_uk': 1}, 'total_duration': 7926281816, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'eu': 1, 'ca': 2}, 'number_of_visits': 3, 'os': {'Linux': 2, 'Windows': 1}, 'number_of_pageviews': 6396115447, 'browser': {'IE 1': 1, 'FF 0': 1, 'FF 2': 1}}, 'domain': 'domain_name_30'}
EXPECTED_SITE_HOURLY_30 = {'timeperiod': '2001030311', 'stat': {'language': {'en_ca': 2, 'ua_uk': 1}, 'total_duration': 8917680474, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'eu': 1, 'ca': 2}, 'number_of_visits': 3, 'os': {'Linux': 2, 'Windows': 1}, 'number_of_pageviews': 9014027568, 'browser': {'IE 1': 1, 'FF 0': 1, 'FF 2': 1}}, 'domain': 'domain_name_24'}
EXPECTED_SITE_HOURLY_31 = {'timeperiod': '2001030310', 'stat': {'language': {'en_ca': 1, 'ua_uk': 2}, 'total_duration': 6279505257, 'screen_resolution': {'(360, 480)': 1, '(760, 980)': 1, '(240, 360)': 1}, 'country': {'ca': 1, 'eu': 2}, 'number_of_visits': 3, 'os': {'Linux': 1, 'Windows': 2}, 'number_of_pageviews': 6415760870, 'browser': {'IE 0': 1, 'IE 2': 1, 'FF 2': 1}}, 'domain': 'domain_name_3'}
EXPECTED_SITE_HOURLY_32 = {'timeperiod': '2001030310', 'stat': {'language': {'en_ca': 1, 'ua_uk': 2}, 'total_duration': 5873567722, 'screen_resolution': {'(360, 480)': 1, '(760, 980)': 1, '(240, 360)': 1}, 'country': {'ca': 1, 'eu': 2}, 'number_of_visits': 3, 'os': {'Linux': 1, 'Windows': 2}, 'number_of_pageviews': 4815289323, 'browser': {'IE 8': 1, 'IE 6': 1, 'FF 2': 1}}, 'domain': 'domain_name_11'}
EXPECTED_SITE_HOURLY_33 = {'timeperiod': '2001030310', 'stat': {'language': {'en_ca': 1, 'ua_uk': 2}, 'total_duration': 7639768213, 'screen_resolution': {'(760, 980)': 2, '(240, 360)': 1}, 'country': {'ca': 1, 'eu': 2}, 'number_of_visits': 3, 'os': {'Linux': 1, 'Windows': 2}, 'number_of_pageviews': 6722749284, 'browser': {'IE 0': 1, 'IE 2': 1, 'FF 2': 1}}, 'domain': 'domain_name_15'}
# pylint: enable=C0301


def generate_session_composite_key(index, total):
    timeperiod = '2001030310' if index <= total / 2 else '2001030311'
    return 'domain_name_{0}'.format(index // 3), timeperiod, 'session_id_{0}'.format(index)


def clean_session_entries():
    logger = get_logger(PROCESS_UNIT_TEST)
    ss_dao = SingleSessionDao(logger)
    for i in range(base_fixtures.TOTAL_ENTRIES):
        key = generate_session_composite_key(i, base_fixtures.TOTAL_ENTRIES)
        ss_dao.remove(key)


def generated_session_entries():
    return base_fixtures.create_session_stats(generate_session_composite_key)


def clean_site_entries():
    return base_fixtures.clean_site_entries(COLLECTION_SITE_HOURLY, QUALIFIER_HOURLY)


def generated_site_entries():
    return base_fixtures.create_site_stats(COLLECTION_SITE_HOURLY, QUALIFIER_HOURLY)


if __name__ == '__main__':
    pass
