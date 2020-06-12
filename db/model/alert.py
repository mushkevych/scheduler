__author__ = 'Bohdan Mushkevych'

from db.model.raw_data import *
from odm.fields import StringField, IntegerField, ObjectIdField


class Alert(BaseDocument):
    """
    class presents model describing an Alert event, such as 20% increase or decrease in the page views
    """

    db_id = ObjectIdField(name='_id', null=True)
    domain_name = StringField(name='domain')
    timeperiod = StringField()      # YYYYMMDDHH format
    number_of_pageviews = IntegerField()
    number_of_visits = IntegerField()

    @classmethod
    def key_fields(cls):
        return cls.domain_name.name, cls.timeperiod.name

TIMEPERIOD = Alert.timeperiod.name
DOMAIN_NAME = Alert.domain_name.name
