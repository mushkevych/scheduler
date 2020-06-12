__author__ = 'Bohdan Mushkevych'

from odm.fields import ObjectIdField, DictField, NestedDocumentField
from db.model.raw_data import *


class NestedStat(BaseDocument):
    number_of_pageviews = IntegerField(default=0)
    total_duration = IntegerField(default=0)
    number_of_visits = IntegerField(default=0)
    os = DictField()
    browser = DictField()
    screen_resolution = DictField()
    language = DictField()
    country = DictField()


class SiteStatistics(BaseDocument):
    """
    class presents site statistics, such as number of visits per defined period or list of search keywords
    """

    db_id = ObjectIdField(name='_id', null=True)
    domain_name = StringField(name='domain')
    timeperiod = StringField()
    stat = NestedDocumentField(NestedStat)

    @classmethod
    def key_fields(cls):
        return cls.domain_name.name, cls.timeperiod.name


TIMEPERIOD = SiteStatistics.timeperiod.name
DOMAIN_NAME = SiteStatistics.domain_name.name
