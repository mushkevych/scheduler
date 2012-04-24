"""
Created on 2011-01-24

@author: Bohdan Mushkevych
"""

from model.abstract_model import AbstractModel

class RawData(AbstractModel):
    SESSION = 'session'
    USER = 'user'
    DOMAIN_NAME = 'domain'
    PAGE_VIEW = 'page_view'
    SCREEN_X = 'screen_x'
    SCREEN_Y = 'screen_y'

    def __init__(self, document = None):
        super(RawData, self).__init__(document)
    
    def composite_key(self, domain_name, timestamp):
        self.data[self.DOMAIN_NAME] = domain_name
        self.data[self.TIMESTAMP] = timestamp

    def get_key(self):
        return self.data[self.DOMAIN_NAME], self.data[self.TIMESTAMP]

    def set_session_id(self, value):
        self.data[self.SESSION] = value
        
    def get_session_id(self):
        return self.data[self.SESSION]

    def set_ip(self, value):
        self.data[self.IP] = value

    def get_ip(self):
        return self.data[self.IP]

    def set_screen_res(self, value_x, value_y):
        self.data[self.SCREEN_X] = value_x
        self.data[self.SCREEN_Y] = value_y
        
    def get_screen_res(self):
        return self.data.get(self.SCREEN_X), self.data.get(self.SCREEN_Y)

    def set_os(self, value):
        self.data[self.OS] = value

    def get_os(self):
        return self.data.get(self.OS)

    def set_browser(self, value):
        self.data[self.BROWSER] = value

    def get_browser(self):
        return self.data.get(self.BROWSER)

    def set_language(self, value):
        self.data[self.LANGUAGE] = value
        
    def get_language(self):
        return self.data.get(self.LANGUAGE)

    def set_country(self, value):
        self.data[self.COUNTRY] = value

    def get_country(self):
        return self.data.get(self.COUNTRY)

    def set_page_view(self):
        self.data[self.PAGE_VIEW] = 1

    def is_page_view(self):
        if self.PAGE_VIEW in self.data:
            return True
        return False
