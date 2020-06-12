__author__ = 'Bohdan Mushkevych'

import random

from db.model.site_statistics import SiteStatistics, TIMEPERIOD
from db.model.client_statistics import ClientStatistics, CLIENT_ID
from synergy.system.utils import copy_and_sum_families
from synergy.system import time_helper
from workers.abstract_mongo_worker import AbstractMongoWorker

random.seed(9001)


class ClientDailyAggregator(AbstractMongoWorker):
    """ illustration suite worker:
        - an aggregator from the site_daily into the client_daily """

    def __init__(self, process_name):
        super(ClientDailyAggregator, self).__init__(process_name)

    def _init_sink_key(self, *args):
        return args[0], time_helper.hour_to_day(args[1])

    def _mongo_sink_key(self, *args):
        return {CLIENT_ID: args[0], TIMEPERIOD: args[1]}

    def _init_source_object(self, document):
        return SiteStatistics.from_json(document)

    def _init_sink_object(self, composite_key):
        obj = ClientStatistics()
        obj.key = composite_key
        return obj

    def _process_single_document(self, document):
        source_obj = self._init_source_object(document)
        try:
            client_id = random.randint(1, 100)

            composite_key = self._init_sink_key(client_id, source_obj.timeperiod)
            target_obj = self._get_aggregated_object(composite_key)

            target_obj.number_of_visits += source_obj.number_of_visits
            target_obj.number_of_pageviews += source_obj.number_of_pageviews
            target_obj.total_duration += source_obj.total_duration
            copy_and_sum_families(source_obj.os, target_obj.os)
            copy_and_sum_families(source_obj.browsers, target_obj.browsers)
            copy_and_sum_families(source_obj.screen_res, target_obj.screen_res)
            copy_and_sum_families(source_obj.languages, target_obj.languages)
            copy_and_sum_families(source_obj.countries, target_obj.countries)
        except KeyError:
            self.logger.error(f'domain name {source_obj.key[0]} has no valid owner client_id')


if __name__ == '__main__':
    from constants import PROCESS_CLIENT_DAILY

    source = ClientDailyAggregator(PROCESS_CLIENT_DAILY)
    source.start()
