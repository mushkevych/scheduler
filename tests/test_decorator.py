__author__ = 'Bohdan Mushkevych'

import unittest

from model.web_scrape import WebScrape
from model.app_package_state import AppPackageState
from model.mq_scrape_wrapper import MqScrapeWrapper, FIELD_APP_PACKAGE_STATE, FIELD_SCRAPE_CONTENT
from tests.utils import create_app_package_state, create_web_scrape


class TestDecorator(unittest.TestCase):
    def test_setter(self):
        mq_scrape_wrapper = MqScrapeWrapper()
        web_scrape = create_web_scrape()
        app_package_state = create_app_package_state()

        mq_scrape_wrapper.app_package_state = app_package_state
        mq_scrape_wrapper.scrape_content = web_scrape

        self.assertIsInstance(mq_scrape_wrapper.app_package_state, AppPackageState)
        self.assertIsInstance(mq_scrape_wrapper.scrape_content, WebScrape)
        self.assertIsInstance(mq_scrape_wrapper.data[FIELD_APP_PACKAGE_STATE], dict)
        self.assertIsInstance(mq_scrape_wrapper.data[FIELD_SCRAPE_CONTENT], dict)

    def test_getter(self):
        mq_scrape_wrapper = MqScrapeWrapper()
        web_scrape = create_web_scrape()
        app_package_state = create_app_package_state()

        mq_scrape_wrapper.app_package_state = app_package_state.data
        mq_scrape_wrapper.scrape_content = web_scrape.data

        self.assertIsInstance(mq_scrape_wrapper.app_package_state, AppPackageState)
        self.assertIsInstance(mq_scrape_wrapper.scrape_content, WebScrape)
        self.assertIsInstance(mq_scrape_wrapper.data[FIELD_APP_PACKAGE_STATE], dict)
        self.assertIsInstance(mq_scrape_wrapper.data[FIELD_SCRAPE_CONTENT], dict)


if __name__ == '__main__':
    unittest.main()
