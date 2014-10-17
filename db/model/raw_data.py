__author__ = 'Bohdan Mushkevych'

from synergy.db.model.base_model import *


CREATION_TIME = 'creation_time'
SESSION_ID = 'session_id'
IP = 'ip'
OS = 'os'
BROWSER = 'browser'
USER_ID = 'user_id'
SCREEN_RESOLUTION_X = 'screen_resolution_x'
SCREEN_RESOLUTION_Y = 'screen_resolution_y'
LANGUAGE = 'language'
COUNTRY = 'country'
PAGE = 'page'

SESSION = 'session'
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


class RawData(BaseModel):
    """ Non-persistent model. Instance of this class presents single message to the SingleSessionWorker """
    def __init__(self, document=None):
        super(RawData, self).__init__(document)

    @BaseModel.key.getter
    def key(self):
        return self.data[DOMAIN_NAME], self.data[TIMEPERIOD], self.session_id

    @key.setter
    def key(self, value):
        self.data[DOMAIN_NAME] = value[0]
        self.data[TIMEPERIOD] = value[1]
        self.session_id = value[2]

    @property
    def session_id(self):
        return self.data[SESSION]

    @session_id.setter
    def session_id(self, value):
        self.data[SESSION] = value

    @property
    def ip(self):
        return self.data[IP]

    @ip.setter
    def ip(self, value):
        self.data[IP] = value

    @property
    def screen_res(self):
        return self.data.get(SCREEN_X), self.data.get(SCREEN_Y)

    @screen_res.setter
    def screen_res(self, value):
        self.data[SCREEN_X] = value[0]
        self.data[SCREEN_Y] = value[1]

    @property
    def os(self):
        return self.data.get(OS)

    @os.setter
    def os(self, value):
        self.data[OS] = value

    @property
    def browser(self):
        return self.data.get(BROWSER)

    @browser.setter
    def browser(self, value):
        self.data[BROWSER] = value

    @property
    def language(self):
        return self.data.get(LANGUAGE)

    @language.setter
    def language(self, value):
        self.data[LANGUAGE] = value

    @property
    def country(self):
        return self.data.get(COUNTRY)

    @country.setter
    def country(self, value):
        self.data[COUNTRY] = value

    @property
    def is_page_view(self):
        if PAGE_VIEW in self.data:
            return True
        return False

    @is_page_view.setter
    def is_page_view(self, _):
        self.data[PAGE_VIEW] = 1
