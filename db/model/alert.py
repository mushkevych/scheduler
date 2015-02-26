__author__ = 'Bohdan Mushkevych'

from db.model.raw_data import *
from odm.fields import StringField, IntegerField, ObjectIdField


class Alert(BaseDocument):
    """
    class presents model describing an Alert event, such as 20% increase or decrease in the page views
    """

    db_id = ObjectIdField('_id', null=True)
    domain_name = StringField(DOMAIN_NAME)
    timeperiod = StringField(TIMEPERIOD)
    number_of_pageviews = IntegerField(NUMBER_OF_PAGEVIEWS)
    number_of_visits = IntegerField(NUMBER_OF_VISITS)

    @BaseDocument.key.getter
    def key(self):
        return self.domain_name, self.timeperiod

    @key.setter
    def key(self, value):
        """
        :param value: tuple (domain_name <string>, timeperiod <string in YYYYMMDDHH format>)
        """
        self.domain_name = value[0]
        self.timeperiod = value[1]
