__author__ = 'Bohdan Mushkevych'

from db.model.single_session import SingleSession
from db.model.site_statistics import SiteStatistics
from db.model.base_model import BaseModel
from settings import settings
from workers.abstract_vertical_worker import AbstractVerticalWorker
from system import time_helper


class SiteHourlyAggregator(AbstractVerticalWorker):
    """
    class works as an aggregator from the single_session_collection and produces/updates records in the hourly_variant_collection
    principle of work is following: we extract all of the sessions for the hour (for example: from 13:00:00 till 13:59:59) 
    and aggregate them into one record of hourly_variant_collection
    """

    def __init__(self, process_name):
        super(SiteHourlyAggregator, self).__init__(process_name)

    def _get_tunnel_port(self):
        return settings['tunnel_site_port']

    def _init_target_key(self, *args):
        """ abstract method to create composite key from source domain_name and timeperiod"""
        return args[0], time_helper.session_to_hour(args[1])

    def _init_source_object(self, document):
        """ abstract method to initialise object with map from source collection """
        return SingleSession(document)

    def _init_target_object(self, composite_key):
        """ abstract method to instantiate new object that will be holding aggregated data """
        obj = SiteStatistics()
        obj.key = (composite_key[0], composite_key[1])
        obj.number_of_visits = 0
        return obj

    def _process_single_document(self, document):
        """ abstract method that actually processes the document from source collection"""
        source_obj = self._init_source_object(document)
        composite_key = self._init_target_key(source_obj.key[0], source_obj.key[1])
        target_obj = self._get_aggregated_object(composite_key)

        target_obj.number_of_visits += 1
        target_obj.number_of_pageviews += source_obj.number_of_pageviews
        target_obj.total_duration += source_obj.total_duration
        BaseModel._increment_family_property(source_obj.os, target_obj.os)
        BaseModel._increment_family_property(source_obj.browser, target_obj.browsers)
        BaseModel._increment_family_property(source_obj.screen_res, target_obj.screen_res)
        BaseModel._increment_family_property(source_obj.language, target_obj.languages)
        BaseModel._increment_family_property(source_obj.country, target_obj.countries)


if __name__ == '__main__':
    from constants import PROCESS_SITE_HOURLY

    source = SiteHourlyAggregator(PROCESS_SITE_HOURLY)
    source.start()
