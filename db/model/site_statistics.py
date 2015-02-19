__author__ = 'Bohdan Mushkevych'

from odm.fields import ObjectIdField, DictField, NestedDocumentField
from db.model.raw_data import *


class NestedStat(BaseDocument):
    number_of_pageviews = IntegerField(NUMBER_OF_PAGEVIEWS, default=0)
    total_duration = IntegerField(TOTAL_DURATION, default=0)
    number_of_visits = IntegerField(NUMBER_OF_VISITS, default=0)
    os = DictField(FAMILY_OS)
    browsers = DictField(FAMILY_BROWSERS)
    screen_res = DictField(FAMILY_SCREEN_RESOLUTIONS)
    languages = DictField(FAMILY_LANGUAGES)
    countries = DictField(FAMILY_COUNTRIES)


class SiteStatistics(BaseDocument):
    """
    class presents site statistics, such as number of visits per defined period or list of search keywords
    """

    db_id = ObjectIdField('_id', null=True)
    domain_name = StringField(DOMAIN_NAME)
    timeperiod = StringField(TIMEPERIOD)
    stat = NestedDocumentField(FAMILY_STAT, NestedStat)

    @BaseModel.key.getter
    def key(self):
        return self.domain_name, self.timeperiod

    @key.setter
    def key(self, value):
        self.domain_name = value[0]
        self.timeperiod = value[1]
