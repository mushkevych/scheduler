__author__ = 'Bohdan Mushkevych'

from odm.fields import ObjectIdField
from db.model.raw_data import *
from synergy.db.model.base_model import *

TIMESTAMP = 'timestamp'


class SingleSession(BaseModel):
    """
    class presents statistics, gathered during the life of the session
    """

    db_id = ObjectIdField('_id', null=True)
    domain_name = StringField(DOMAIN_NAME)
    timeperiod = StringField(TIMEPERIOD)
    session_id = StringField(SESSION_ID)

    @BaseModel.key.getter
    def key(self):
        return self.domain_name, self.timeperiod, self.session_id

    @key.setter
    def key(self, value):
        self.domain_name = value[0]
        self.timeperiod = value[1]
        self.session_id = value[2]

    @property
    def session_id(self):
        family_column = self._get_column_family(FAMILY_USER_PROFILE)
        return family_column[SESSION_ID]

    @session_id.setter
    def session_id(self, value):
        family_column = self._get_column_family(FAMILY_USER_PROFILE)
        family_column[SESSION_ID] = value

    @property
    def ip(self):
        family_column = self._get_column_family(FAMILY_USER_PROFILE)
        return family_column[IP]

    @ip.setter
    def ip(self, value):
        family_column = self._get_column_family(FAMILY_USER_PROFILE)
        family_column[IP] = value

    @property
    def os(self):
        family_column = self._get_column_family(FAMILY_USER_PROFILE)
        return family_column.get(OS)

    @os.setter
    def os(self, value):
        family_column = self._get_column_family(FAMILY_USER_PROFILE)
        family_column[OS] = value

    @property
    def browser(self):
        family_column = self._get_column_family(FAMILY_USER_PROFILE)
        return family_column.get(BROWSER)

    @browser.setter
    def browser(self, value):
        family_column = self._get_column_family(FAMILY_USER_PROFILE)
        family_column[BROWSER] = value

    @property
    def screen_res(self):
        family_column = self._get_column_family(FAMILY_USER_PROFILE)
        return family_column.get(SCREEN_X), family_column.get(SCREEN_Y)

    @screen_res.setter
    def screen_res(self, value):
        family_column = self._get_column_family(FAMILY_USER_PROFILE)
        family_column[SCREEN_X] = value[0]
        family_column[SCREEN_Y] = value[1]

    @property
    def language(self):
        family_column = self._get_column_family(FAMILY_USER_PROFILE)
        return family_column.get(LANGUAGE)

    @language.setter
    def language(self, value):
        family_column = self._get_column_family(FAMILY_USER_PROFILE)
        family_column[LANGUAGE] = value

    @property
    def country(self):
        family_column = self._get_column_family(FAMILY_USER_PROFILE)
        return family_column.get(COUNTRY)

    @country.setter
    def country(self, value):
        family_column = self._get_column_family(FAMILY_USER_PROFILE)
        family_column[COUNTRY] = value

    @property
    def total_duration(self):
        family_column = self._get_column_family(FAMILY_BROWSING_HISTORY)
        if TOTAL_DURATION not in family_column:
            return 0
        else:
            return family_column.get(TOTAL_DURATION)

    @total_duration.setter
    def total_duration(self, value):
        family_column = self._get_column_family(FAMILY_BROWSING_HISTORY)
        family_column[TOTAL_DURATION] = value

    @property
    def number_of_pageviews(self):
        family_column = self._get_column_family(FAMILY_BROWSING_HISTORY)
        return family_column.get(NUMBER_OF_PAGEVIEWS)

    @number_of_pageviews.setter
    def number_of_pageviews(self, value):
        family_column = self._get_column_family(FAMILY_BROWSING_HISTORY)
        family_column[NUMBER_OF_PAGEVIEWS] = value

    @property
    def number_of_entries(self):
        family_column = self._get_column_family(FAMILY_BROWSING_HISTORY)
        return family_column.get(NUMBER_OF_ENTRIES)

    @number_of_entries.setter
    def number_of_entries(self, value):
        family_column = self._get_column_family(FAMILY_BROWSING_HISTORY)
        family_column[NUMBER_OF_ENTRIES] = value

    def _get_entry(self, entry_id):
        """ entry_id is numerical index """
        browsing_history = self._get_column_family(FAMILY_BROWSING_HISTORY)
        if FAMILY_ENTRIES not in browsing_history:
            browsing_history[FAMILY_ENTRIES] = []
        if len(browsing_history[FAMILY_ENTRIES]) <= entry_id:
            for _ in range(entry_id - len(browsing_history[FAMILY_ENTRIES]) + 1):
                browsing_history[FAMILY_ENTRIES].append(dict())
        return browsing_history[FAMILY_ENTRIES][entry_id]

    def set_entry_timestamp(self, entry_id, value):
        entry = self._get_entry(entry_id)
        entry[TIMESTAMP] = value

    def get_entry_timestamp(self, entry_id):
        entry = self._get_entry(entry_id)
        return entry[TIMESTAMP]
