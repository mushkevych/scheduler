__author__ = 'Bohdan Mushkevych'

from odm.fields import ObjectIdField, NestedDocumentField, DictField
from db.model.raw_data import *


class NestedUserProfile(BaseDocument):
    ip = StringField()
    os = StringField()
    browser = StringField()
    language = StringField()
    country = StringField()
    screen_x = IntegerField()
    screen_y = IntegerField()

    @property
    def screen_resolution(self):
        return self.screen_x, self.screen_y

    @screen_resolution.setter
    def screen_resolution(self, value):
        self.screen_x = value[0]
        self.screen_y = value[1]


class NestedBrowsingHistory(BaseDocument):
    total_duration = IntegerField(default=0)
    number_of_pageviews = IntegerField(default=0)
    number_of_entries = IntegerField(default=0)
    entries_timestamps = DictField()

    def set_entry_timestamp(self, entry_id, value):
        if not isinstance(entry_id, str):
            entry_id = str(entry_id)
        self.entries_timestamps[entry_id] = value

    def get_entry_timestamp(self, entry_id):
        if not isinstance(entry_id, str):
            entry_id = str(entry_id)
        return self.entries_timestamps[entry_id]


class SingleSession(BaseDocument):
    """
    class presents statistics, gathered during the life of the session
    """

    db_id = ObjectIdField(name='_id', null=True)
    domain_name = StringField(name='domain')
    timeperiod = StringField()
    session_id = StringField()
    user_profile = NestedDocumentField(NestedUserProfile)
    browsing_history = NestedDocumentField(NestedBrowsingHistory)

    @classmethod
    def key_fields(cls):
        return cls.domain_name.name, cls.timeperiod.name, cls.session_id.name


SESSION_ID = SingleSession.session_id.name
TIMEPERIOD = SingleSession.timeperiod.name
DOMAIN_NAME = SingleSession.domain_name.name
