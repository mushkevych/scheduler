from synergy.db.model import job

__author__ = 'Bohdan Mushkevych'

import unittest
from rest_client.restful_lib import Connection

from synergy.conf import settings
from constants import PROCESS_ALERT_DAILY, PROCESS_SITE_HOURLY, PROCESS_CLIENT_DAILY


class TestSchedulerMx(unittest.TestCase):
    REQUEST_CHILDREN = '/request_children/'
    REQUEST_SCHED_DETAILS = '/scheduler_details/'
    REQUEST_VERTICAL_DETAILS = '/request_verticals/'

    def setUp(self):
        host = settings.settings['mx_host']
        port = settings.settings['mx_port']
        self.connection = Connection(base_url='http://' + host + ':' + str(port))

    def tearDown(self):
        pass

    def test_request_root(self):
        resp = self.connection.request_get(
            self.REQUEST_CHILDREN,
            args={job.PROCESS_NAME: PROCESS_SITE_HOURLY},
            headers={'content-type': 'application/json', 'accept': 'application/json'})
        status = resp[u'headers']['status']

        # check that we either got a successful response (200) or a previously retrieved, but still valid response (304)
        if status == '200' or status == '304':
            print(resp[u'body'])
        else:
            print('Request failed with status %r' % status)

    def test_request_4_level_children(self):
        resp = self.connection.request_get(
            self.REQUEST_CHILDREN,
            args={job.TIMEPERIOD: settings.settings['synergy_start_timeperiod'],
                  job.PROCESS_NAME: PROCESS_SITE_HOURLY},
            headers={'content-type': 'application/json', 'accept': 'application/json'})
        status = resp[u'headers']['status']

        # check that we either got a successful response (200) or a previously retrieved, but still valid response (304)
        if status == '200' or status == '304':
            print(resp[u'body'])
        else:
            print('Request failed with status %r' % status)

    def test_request_3_level_children(self):
        resp = self.connection.request_get(
            self.REQUEST_CHILDREN,
            args={job.TIMEPERIOD: settings.settings['synergy_start_timeperiod'],
                  job.PROCESS_NAME: PROCESS_CLIENT_DAILY},
            headers={'content-type': 'application/json', 'accept': 'application/json'})
        status = resp[u'headers']['status']

        # check that we either got a successful response (200) or a previously retrieved, but still valid response (304)
        if status == '200' or status == '304':
            print(resp[u'body'])
        else:
            print('Request failed with status %r' % status)

    def test_request_1_level_children(self):
        resp = self.connection.request_get(
            self.REQUEST_CHILDREN,
            args={job.PROCESS_NAME: PROCESS_ALERT_DAILY},
            headers={'content-type': 'application/json', 'accept': 'application/json'})
        status = resp[u'headers']['status']

        # check that we either got a successful response (200) or a previously retrieved, but still valid response (304)
        if status == '200' or status == '304':
            print(resp[u'body'])
        else:
            print('Request failed with status %r' % status)

    def test_request_verticals(self):
        resp = self.connection.request_get(
            self.REQUEST_VERTICAL_DETAILS,
            args={},
            headers={'content-type': 'application/json', 'accept': 'application/json'})
        status = resp[u'headers']['status']

        # check that we either got a successful response (200) or a previously retrieved, but still valid response (304)
        if status == '200' or status == '304':
            print(resp[u'body'])
        else:
            print('Request failed with status %r' % status)


if __name__ == '__main__':
    unittest.main()