__author__ = 'Bohdan Mushkevych'

from db.model.raw_data import DOMAIN_NAME
from db.model.site_statistics import SiteStatistics
from db.model.client_statistics import ClientStatistics
from workers.abstract_horizontal_worker import AbstractHorizontalWorker
from synergy.db.model.base_model import BaseModel
from synergy.conf import settings
from synergy.system import time_helper
from synergy.system.restful_client import RestClient


class ClientDailyAggregator(AbstractHorizontalWorker):
    """ class works as an aggregator from the site_hourly into the site_daily """

    def __init__(self, process_name):
        super(ClientDailyAggregator, self).__init__(process_name)
        self.rest_client = RestClient(self.logger)

    def _get_tunnel_port(self):
        return settings.settings['tunnel_site_port']

    def _init_sink_key(self, *args):
        return args[0], time_helper.hour_to_day(args[1])

    def _init_source_object(self, document):
        return SiteStatistics.from_json(document)

    def _init_sink_object(self, composite_key):
        obj = ClientStatistics()
        obj.key = (composite_key[0], composite_key[1])
        return obj

    def _process_bulk_array(self, array_of_documents, timeperiod):
        domain_list = [x[DOMAIN_NAME] for x in array_of_documents]
        client_mapping = self.rest_client.get_client_mapping(timeperiod, domain_list)

        for document in array_of_documents:
            source_obj = self._init_source_object(document)
            try:
                client_id = client_mapping[source_obj.key[0]]

                composite_key = self._init_sink_key(client_id, timeperiod)
                target_obj = self._get_aggregated_object(composite_key)

                target_obj.number_of_visits += source_obj.number_of_visits
                target_obj.number_of_pageviews += source_obj.number_of_pageviews
                target_obj.total_duration += source_obj.total_duration
                BaseModel._copy_and_sum_families(source_obj.os, target_obj.os)
                BaseModel._copy_and_sum_families(source_obj.browsers, target_obj.browsers)
                BaseModel._copy_and_sum_families(source_obj.screen_res, target_obj.screen_res)
                BaseModel._copy_and_sum_families(source_obj.languages, target_obj.languages)
                BaseModel._copy_and_sum_families(source_obj.countries, target_obj.countries)
            except KeyError:
                self.logger.error('domain name %s has no valid owner client_id' % source_obj.key[0])


if __name__ == '__main__':
    from constants import PROCESS_CLIENT_DAILY

    source = ClientDailyAggregator(PROCESS_CLIENT_DAILY)
    source.start()
