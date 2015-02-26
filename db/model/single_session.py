__author__ = 'Bohdan Mushkevych'

from odm.fields import ObjectIdField, NestedDocumentField, DictField
from db.model.raw_data import *


class NestedUserProfile(BaseDocument):
    ip = StringField(IP)
    os = StringField(OS)
    browser = StringField(BROWSER)
    language = StringField(LANGUAGE)
    country = StringField(COUNTRY)
    screen_x = IntegerField(SCREEN_X)
    screen_y = IntegerField(SCREEN_Y)

    @property
    def screen_res(self):
        return self.screen_x, self.screen_y

    @screen_res.setter
    def screen_res(self, value):
        self.screen_x = value[0]
        self.screen_y = value[1]


class NestedBrowsingHistory(BaseDocument):
    total_duration = IntegerField(TOTAL_DURATION, default=0)
    number_of_pageviews = IntegerField(NUMBER_OF_PAGEVIEWS, default=0)
    number_of_entries = IntegerField(NUMBER_OF_ENTRIES, default=0)
    entries_timestamps = DictField(FAMILY_ENTRIES)

    def set_entry_timestamp(self, entry_id, value):
        if not isinstance(entry_id, basestring):
            entry_id = str(entry_id)
        self.entries_timestamps[entry_id] = value

    def get_entry_timestamp(self, entry_id):
        if not isinstance(entry_id, basestring):
            entry_id = str(entry_id)
        return self.entries_timestamps[entry_id]


class SingleSession(BaseDocument):
    """
    class presents statistics, gathered during the life of the session
    """

    db_id = ObjectIdField('_id', null=True)
    domain_name = StringField(DOMAIN_NAME)
    timeperiod = StringField(TIMEPERIOD)
    session_id = StringField(SESSION_ID)
    user_profile = NestedDocumentField(FAMILY_USER_PROFILE, NestedUserProfile)
    browsing_history = NestedDocumentField(FAMILY_BROWSING_HISTORY, NestedBrowsingHistory)

    @BaseDocument.key.getter
    def key(self):
        return self.domain_name, self.timeperiod, self.session_id

    @key.setter
    def key(self, value):
        self.domain_name = value[0]
        self.timeperiod = value[1]
        self.session_id = value[2]
