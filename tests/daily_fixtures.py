import base_fixtures
from constants import COLLECTION_SITE_DAILY
from synergy.system.time_qualifier import QUALIFIER_DAILY

__author__ = 'Bohdan Mushkevych'

# pylint: disable=C0301
EXPECTED_SITE_DAILY_00 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 152,
         'number_of_visits': 2234, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 186, u'os_2': 186, u'os_1': 186, u'os_0': 186, u'os_4': 186},
         'browser': {u'browser_4': 186, u'browser_0': 186, u'browser_1': 186, u'browser_2': 186, u'browser_3': 186}},
'domain': u'domain_name_29', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_01 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 222,
         'number_of_visits': 1247, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 165, u'os_2': 165, u'os_1': 165, u'os_0': 165, u'os_4': 165},
         'browser': {u'browser_4': 165, u'browser_0': 165, u'browser_1': 165, u'browser_2': 165, u'browser_3': 165}},
'domain': u'domain_name_22', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_02 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 259,
         'number_of_visits': 1700, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 159, u'os_2': 159, u'os_1': 159, u'os_0': 159, u'os_4': 159},
         'browser': {u'browser_4': 159, u'browser_0': 159, u'browser_1': 159, u'browser_2': 159, u'browser_3': 159}},
'domain': u'domain_name_20', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_03 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 211,
         'number_of_visits': 1796, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 144, u'os_2': 144, u'os_1': 144, u'os_0': 144, u'os_4': 144},
         'browser': {u'browser_4': 144, u'browser_0': 144, u'browser_1': 144, u'browser_2': 144, u'browser_3': 144}},
'domain': u'domain_name_15', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_04 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 133,
         'number_of_visits': 1896, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 114, u'os_2': 114, u'os_1': 114, u'os_0': 114, u'os_4': 114},
         'browser': {u'browser_4': 114, u'browser_0': 114, u'browser_1': 114, u'browser_2': 114, u'browser_3': 114}},
'domain': u'domain_name_5', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_05 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 222,
         'number_of_visits': 1924, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 105, u'os_2': 105, u'os_1': 105, u'os_0': 105, u'os_4': 105},
         'browser': {u'browser_4': 105, u'browser_0': 105, u'browser_1': 105, u'browser_2': 105, u'browser_3': 105}},
'domain': u'domain_name_2', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_06 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 168,
         'number_of_visits': 1856, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 120, u'os_2': 120, u'os_1': 120, u'os_0': 120, u'os_4': 120},
         'browser': {u'browser_4': 120, u'browser_0': 120, u'browser_1': 120, u'browser_2': 120, u'browser_3': 120}},
'domain': u'domain_name_7', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_07 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 81,
         'number_of_visits': 1595, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 126, u'os_2': 126, u'os_1': 126, u'os_0': 126, u'os_4': 126},
         'browser': {u'browser_4': 126, u'browser_0': 126, u'browser_1': 126, u'browser_2': 126, u'browser_3': 126}},
'domain': u'domain_name_9', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_08 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 168,
         'number_of_visits': 1307, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 111, u'os_2': 111, u'os_1': 111, u'os_0': 111, u'os_4': 111},
         'browser': {u'browser_4': 111, u'browser_0': 111, u'browser_1': 111, u'browser_2': 111, u'browser_3': 111}},
'domain': u'domain_name_4', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_09 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 101,
         'number_of_visits': 792, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 153, u'os_2': 153, u'os_1': 153, u'os_0': 153, u'os_4': 153},
         'browser': {u'browser_4': 153, u'browser_0': 153, u'browser_1': 153, u'browser_2': 153, u'browser_3': 153}},
'domain': u'domain_name_18', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_10 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 119,
         'number_of_visits': 1807, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 177, u'os_2': 177, u'os_1': 177, u'os_0': 177, u'os_4': 177},
         'browser': {u'browser_4': 177, u'browser_0': 177, u'browser_1': 177, u'browser_2': 177, u'browser_3': 177}},
'domain': u'domain_name_26', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_11 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 214,
         'number_of_visits': 1927, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 162, u'os_2': 162, u'os_1': 162, u'os_0': 162, u'os_4': 162},
         'browser': {u'browser_4': 162, u'browser_0': 162, u'browser_1': 162, u'browser_2': 162, u'browser_3': 162}},
'domain': u'domain_name_21', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_12 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 111,
         'number_of_visits': 2467, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 189, u'os_2': 189, u'os_1': 189, u'os_0': 189, u'os_4': 189},
         'browser': {u'browser_4': 189, u'browser_0': 189, u'browser_1': 189, u'browser_2': 189, u'browser_3': 189}},
'domain': u'domain_name_30', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_13 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 22,
         'number_of_visits': 834, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 138, u'os_2': 138, u'os_1': 138, u'os_0': 138, u'os_4': 138},
         'browser': {u'browser_4': 138, u'browser_0': 138, u'browser_1': 138, u'browser_2': 138, u'browser_3': 138}},
'domain': u'domain_name_13', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_14 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 159,
         'number_of_visits': 1053, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 141, u'os_2': 141, u'os_1': 141, u'os_0': 141, u'os_4': 141},
         'browser': {u'browser_4': 141, u'browser_0': 141, u'browser_1': 141, u'browser_2': 141, u'browser_3': 141}},
'domain': u'domain_name_14', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_15 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 158,
         'number_of_visits': 1061, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 132, u'os_2': 132, u'os_1': 132, u'os_0': 132, u'os_4': 132},
         'browser': {u'browser_4': 132, u'browser_0': 132, u'browser_1': 132, u'browser_2': 132, u'browser_3': 132}},
'domain': u'domain_name_11', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_16 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 17,
         'number_of_visits': 1404, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 180, u'os_2': 180, u'os_1': 180, u'os_0': 180, u'os_4': 180},
         'browser': {u'browser_4': 180, u'browser_0': 180, u'browser_1': 180, u'browser_2': 180, u'browser_3': 180}},
'domain': u'domain_name_27', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_17 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 121,
         'number_of_visits': 1258, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 117, u'os_2': 117, u'os_1': 117, u'os_0': 117, u'os_4': 117},
         'browser': {u'browser_4': 117, u'browser_0': 117, u'browser_1': 117, u'browser_2': 117, u'browser_3': 117}},
'domain': u'domain_name_6', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_18 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 140,
         'number_of_visits': 486, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 135, u'os_2': 135, u'os_1': 135, u'os_0': 135, u'os_4': 135},
         'browser': {u'browser_4': 135, u'browser_0': 135, u'browser_1': 135, u'browser_2': 135, u'browser_3': 135}},
'domain': u'domain_name_12', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_19 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 126,
         'number_of_visits': 1424, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 123, u'os_2': 123, u'os_1': 123, u'os_0': 123, u'os_4': 123},
         'browser': {u'browser_4': 123, u'browser_0': 123, u'browser_1': 123, u'browser_2': 123, u'browser_3': 123}},
'domain': u'domain_name_8', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_20 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 132,
         'number_of_visits': 1719, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 192, u'os_2': 192, u'os_1': 192, u'os_0': 192, u'os_4': 192},
         'browser': {u'browser_4': 192, u'browser_0': 192, u'browser_1': 192, u'browser_2': 192, u'browser_3': 192}},
'domain': u'domain_name_31', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_21 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 174,
         'number_of_visits': 1993, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 171, u'os_2': 171, u'os_1': 171, u'os_0': 171, u'os_4': 171},
         'browser': {u'browser_4': 171, u'browser_0': 171, u'browser_1': 171, u'browser_2': 171, u'browser_3': 171}},
'domain': u'domain_name_24', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_22 = {
'stat': {'screen_resolution': {u'(320, 240)': 12, u'(640, 480)': 20, u'(1024, 960)': 28, u'(1280, 768)': 36},
         'language': {u'ua_uk': 28, u'ca_en': 12, u'ca_fr': 20, u'us_en': 36}, 'total_duration': 184,
         'number_of_visits': 2102, 'number_of_pageviews': 0, 'country': {u'ca': 12, u'fr': 20, u'uk': 28, u'us': 36},
         'os': {u'os_3': 202, u'os_2': 202, u'os_1': 202, u'os_0': 202, u'os_4': 202},
         'browser': {u'browser_4': 202, u'browser_0': 202, u'browser_1': 202, u'browser_2': 202, u'browser_3': 202}},
'domain': u'domain_name_1', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_23 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 78,
         'number_of_visits': 666, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 129, u'os_2': 129, u'os_1': 129, u'os_0': 129, u'os_4': 129},
         'browser': {u'browser_4': 129, u'browser_0': 129, u'browser_1': 129, u'browser_2': 129, u'browser_3': 129}},
'domain': u'domain_name_10', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_24 = {
'stat': {'screen_resolution': {u'(320, 240)': 12, u'(640, 480)': 20, u'(1024, 960)': 28, u'(1280, 768)': 36},
         'language': {u'ua_uk': 28, u'ca_en': 12, u'ca_fr': 20, u'us_en': 36}, 'total_duration': 234,
         'number_of_visits': 1634, 'number_of_pageviews': 0, 'country': {u'ca': 12, u'fr': 20, u'uk': 28, u'us': 36},
         'os': {u'os_3': 198, u'os_2': 198, u'os_1': 198, u'os_0': 198, u'os_4': 198},
         'browser': {u'browser_4': 198, u'browser_0': 198, u'browser_1': 198, u'browser_2': 198, u'browser_3': 198}},
'domain': u'domain_name_0', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_25 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 236,
         'number_of_visits': 1722, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 183, u'os_2': 183, u'os_1': 183, u'os_0': 183, u'os_4': 183},
         'browser': {u'browser_4': 183, u'browser_0': 183, u'browser_1': 183, u'browser_2': 183, u'browser_3': 183}},
'domain': u'domain_name_28', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_26 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 165,
         'number_of_visits': 1812, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 195, u'os_2': 195, u'os_1': 195, u'os_0': 195, u'os_4': 195},
         'browser': {u'browser_4': 195, u'browser_0': 195, u'browser_1': 195, u'browser_2': 195, u'browser_3': 195}},
'domain': u'domain_name_32', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_27 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 165,
         'number_of_visits': 887, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 150, u'os_2': 150, u'os_1': 150, u'os_0': 150, u'os_4': 150},
         'browser': {u'browser_4': 150, u'browser_0': 150, u'browser_1': 150, u'browser_2': 150, u'browser_3': 150}},
'domain': u'domain_name_17', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_28 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 132,
         'number_of_visits': 1660, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 174, u'os_2': 174, u'os_1': 174, u'os_0': 174, u'os_4': 174},
         'browser': {u'browser_4': 174, u'browser_0': 174, u'browser_1': 174, u'browser_2': 174, u'browser_3': 174}},
'domain': u'domain_name_25', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_29 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 241,
         'number_of_visits': 1617, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 108, u'os_2': 108, u'os_1': 108, u'os_0': 108, u'os_4': 108},
         'browser': {u'browser_4': 108, u'browser_0': 108, u'browser_1': 108, u'browser_2': 108, u'browser_3': 108}},
'domain': u'domain_name_3', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_30 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 202,
         'number_of_visits': 1446, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 168, u'os_2': 168, u'os_1': 168, u'os_0': 168, u'os_4': 168},
         'browser': {u'browser_4': 168, u'browser_0': 168, u'browser_1': 168, u'browser_2': 168, u'browser_3': 168}},
'domain': u'domain_name_23', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_31 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 154,
         'number_of_visits': 2550, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 147, u'os_2': 147, u'os_1': 147, u'os_0': 147, u'os_4': 147},
         'browser': {u'browser_4': 147, u'browser_0': 147, u'browser_1': 147, u'browser_2': 147, u'browser_3': 147}},
'domain': u'domain_name_16', 'timeperiod': '2001030300'}
EXPECTED_SITE_DAILY_32 = {
'stat': {'screen_resolution': {u'(320, 240)': 9, u'(640, 480)': 15, u'(1024, 960)': 21, u'(1280, 768)': 27},
         'language': {u'ua_uk': 21, u'ca_en': 9, u'ca_fr': 15, u'us_en': 27}, 'total_duration': 190,
         'number_of_visits': 1312, 'number_of_pageviews': 0, 'country': {u'ca': 9, u'fr': 15, u'uk': 21, u'us': 27},
         'os': {u'os_3': 156, u'os_2': 156, u'os_1': 156, u'os_0': 156, u'os_4': 156},
         'browser': {u'browser_4': 156, u'browser_0': 156, u'browser_1': 156, u'browser_2': 156, u'browser_3': 156}},
'domain': u'domain_name_19', 'timeperiod': '2001030300'}
# pylint: enable=C0301


def generated_site_entries():
    return base_fixtures.create_site_stats(COLLECTION_SITE_DAILY, QUALIFIER_DAILY)


def clean_site_entries():
    return base_fixtures.clean_site_entries(COLLECTION_SITE_DAILY, QUALIFIER_DAILY)


if __name__ == '__main__':
    pass