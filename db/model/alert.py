__author__ = 'Bohdan Mushkevych'

from synergy.db.model.base_model import *
from db.model.raw_data import *


class Alert(BaseModel):
    """
    class presents model describing an Alert event, such as 20% increase or decrease in the page views
    """

    def __init__(self, document=None):
        super(Alert, self).__init__(document)

    @property
    def key(self):
        return self.data[DOMAIN_NAME], self.data[TIMEPERIOD]

    @key.setter
    def key(self, value):
        """
        :param value: tuple (domain_name <string>, timeperiod <string in YYYYMMDDHH format>)
        """
        self.data[DOMAIN_NAME] = value[0]
        self.data[TIMEPERIOD] = value[1]

    @property
    def number_of_pageviews(self):
        return self.data(NUMBER_OF_PAGEVIEWS, 0)

    @number_of_pageviews.setter
    def number_of_pageviews(self, value):
        self.data[NUMBER_OF_PAGEVIEWS] = value

    @property
    def number_of_visits(self):
        return self.data(NUMBER_OF_VISITS, 0)

    @number_of_visits.setter
    def number_of_visits(self, value):
        self.data[NUMBER_OF_VISITS] = value
