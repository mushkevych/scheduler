__author__ = 'Bohdan Mushkevych'

from db.model.raw_data import DOMAIN_NAME, TIMEPERIOD
from db.model.single_session import SingleSession
from db.model.site_statistics import SiteStatistics
from synergy.system.utils import increment_family_property
from workers.abstract_mongo_worker import AbstractMongoWorker


class SiteHourlyAggregator(AbstractMongoWorker):
    """
    class works as an aggregator from the single_session collection and produces/updates records in the site_hourly
    principle of work is following: we extract all of the sessions for the hour
    (for example: from 13:00:00 till 13:59:59) and aggregate them into one record of site_hourly collection
    """

    def __init__(self, process_name):
        super(SiteHourlyAggregator, self).__init__(process_name)

    def _init_sink_key(self, *args):
        return args[0], args[1]

    def _mongo_sink_key(self, *args):
        return {DOMAIN_NAME: args[0], TIMEPERIOD: args[1]}

    def _init_source_object(self, document):
        return SingleSession.from_json(document)

    def _init_sink_object(self, composite_key):
        obj = SiteStatistics()
        obj.key = composite_key
        return obj

    def _process_single_document(self, document):
        source_obj = self._init_source_object(document)
        composite_key = self._init_sink_key(source_obj.domain_name, source_obj.timeperiod)
        target_obj = self._get_aggregated_object(composite_key)

        target_obj.stat.number_of_visits += 1
        target_obj.stat.number_of_pageviews += source_obj.browsing_history.number_of_pageviews
        target_obj.stat.total_duration += source_obj.browsing_history.total_duration
        increment_family_property(source_obj.user_profile.os, target_obj.stat.os)
        increment_family_property(source_obj.user_profile.browser, target_obj.stat.browsers)
        increment_family_property(source_obj.user_profile.screen_res, target_obj.stat.screen_res)
        increment_family_property(source_obj.user_profile.language, target_obj.stat.languages)
        increment_family_property(source_obj.user_profile.country, target_obj.stat.countries)


if __name__ == '__main__':
    from constants import PROCESS_SITE_HOURLY

    source = SiteHourlyAggregator(PROCESS_SITE_HOURLY)
    source.start()
