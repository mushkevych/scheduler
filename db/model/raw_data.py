__author__ = 'Bohdan Mushkevych'

from odm.document import BaseDocument
from odm.fields import StringField, IntegerField, BooleanField, DateTimeField


class RawData(BaseDocument):
    """ Non-persistent model. Instance of this class presents single message to the SingleSessionWorker """

    domain_name = StringField(name='domain')
    timestamp = DateTimeField()
    session_id = StringField()
    ip = StringField()
    screen_x = IntegerField()
    screen_y = IntegerField()
    os = StringField()
    browser = StringField()
    language = StringField()
    country = StringField()
    is_page_view = BooleanField()

    @classmethod
    def key_fields(cls):
        return cls.domain_name.name, cls.timestamp.name, cls.session_id.name

    @property
    def screen_resolution(self):
        return self.screen_x, self.screen_y

    @screen_resolution.setter
    def screen_resolution(self, value):
        self.screen_x = value[0]
        self.screen_y = value[1]

