__author__ = 'Bohdan Mushkevych'

from werkzeug.utils import cached_property

class ConnectionDetails(object):
    def __init__(self, mbean):
        self.mbean = mbean
        self.logger = self.mbean.logger

    @cached_property
    def entries(self):
        list_of_rows = []
        return list_of_rows
