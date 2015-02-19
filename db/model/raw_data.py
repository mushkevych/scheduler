__author__ = 'Bohdan Mushkevych'

from odm.fields import StringField, IntegerField, BooleanField
from synergy.db.model.base_model import *


CREATION_TIME = 'creation_time'
SESSION_ID = 'session_id'
IP = 'ip'
OS = 'os'
BROWSER = 'browser'
USER_ID = 'user_id'
LANGUAGE = 'language'
COUNTRY = 'country'
PAGE = 'page'

USER = 'user'
DOMAIN_NAME = 'domain'
PAGE_VIEW = 'page_view'
SCREEN_X = 'screen_x'
SCREEN_Y = 'screen_y'

TOTAL_DURATION = 'total_duration'
NUMBER_OF_UNIQUE_VISITORS = 'number_of_uniques'
NUMBER_OF_VISITS = 'number_of_visits'
NUMBER_OF_ENTRIES = 'number_of_entries'
NUMBER_OF_PAGEVIEWS = 'number_of_pageviews'

# FAMILIES
FAMILY_STAT = 'stat'
FAMILY_SITES = 'site'
FAMILY_USER_PROFILE = 'user_profile'
FAMILY_SITE_PROFILE = 'site_profile'
FAMILY_BROWSING_HISTORY = 'browsing_history'
FAMILY_ENTRIES = 'entries'
FAMILY_VISITS = 'visit'
FAMILY_PAGEVIEWS = 'pageview'
FAMILY_DURATION = 'duration'
FAMILY_SESSIONS = 'session'
FAMILY_COUNTRIES = 'country'
FAMILY_OS = 'os'
FAMILY_BROWSERS = 'browser'
FAMILY_SCREEN_RESOLUTIONS = 'screen_resolution'
FAMILY_LANGUAGES = 'language'


class RawData(BaseDocument):
    """ Non-persistent model. Instance of this class presents single message to the SingleSessionWorker """

    domain_name = StringField(DOMAIN_NAME)
    timeperiod = StringField(TIMEPERIOD)
    session_id = StringField(SESSION_ID)
    ip = StringField(IP)
    screen_x = IntegerField(SCREEN_X)
    screen_y = IntegerField(SCREEN_Y)
    os = StringField(OS)
    browser = StringField(BROWSER)
    language = StringField(LANGUAGE)
    country = StringField(COUNTRY)
    is_page_view = BooleanField(PAGE_VIEW)

    @BaseModel.key.getter
    def key(self):
        return self.domain_name, self.timeperiod, self.session_id

    @key.setter
    def key(self, value):
        self.domain_name = value[0]
        self.timeperiod = value[1]
        self.session_id = value[2]

    @property
    def screen_res(self):
        return self.screen_x, self.screen_y

    @screen_res.setter
    def screen_res(self, value):
        self.screen_x = value[0]
        self.screen_y = value[1]
