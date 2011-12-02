"""
Created on 2011-01-25

@author: Bohdan Mushkevych
"""
from data_collections.abstract_collection import AbstractCollection

class SiteStatistics(AbstractCollection):
    """
    class presents site statistics like number of visits per defined period or list of search keywords
    """

    def __init__(self, document = None):
        super(SiteStatistics, self).__init__(document)

    def get_number_of_pageviews(self):
        family_column = self._get_column_family(self.FAMILY_STAT)
        if self.NUMBER_OF_PAGEVIEWS not in family_column:
            family_column[self.NUMBER_OF_PAGEVIEWS] = 0
        return family_column[self.NUMBER_OF_PAGEVIEWS]

    def set_number_of_pageviews(self, value):
        family_column = self._get_column_family(self.FAMILY_STAT)
        family_column[self.NUMBER_OF_PAGEVIEWS] = value
       
    def get_total_duration(self):
        family_column = self._get_column_family(self.FAMILY_STAT)
        if self.TOTAL_DURATION not in family_column:
            family_column[self.TOTAL_DURATION] = 0
        return family_column[self.TOTAL_DURATION]

    def set_total_duration(self, value):
        family_column = self._get_column_family(self.FAMILY_STAT)
        family_column[self.TOTAL_DURATION] = value

    def set_number_of_visits(self, value):
        family_column = self._get_column_family(self.FAMILY_STAT)
        family_column[self.NUMBER_OF_VISITS] = value
        
    def get_number_of_visits(self):
        family_column = self._get_column_family(self.FAMILY_STAT)
        if self.NUMBER_OF_VISITS not in family_column:
            family_column[self.NUMBER_OF_VISITS] = 0
        return family_column[self.NUMBER_OF_VISITS]

    def set_os(self, value):
        family_column = self._get_column_family(self.FAMILY_STAT)
        family_column[self.FAMILY_OS] = value

    def get_os(self):
        family_column = self._get_column_family(self.FAMILY_STAT)
        if self.FAMILY_OS not in family_column:
            family_column[self.FAMILY_OS] = dict()
        return family_column[self.FAMILY_OS]

    def set_browsers(self, value):
        family_column = self._get_column_family(self.FAMILY_STAT)
        family_column[self.FAMILY_BROWSERS] = value

    def get_browsers(self):
        family_column = self._get_column_family(self.FAMILY_STAT)
        if self.FAMILY_BROWSERS not in family_column:
            family_column[self.FAMILY_BROWSERS] = dict()
        return family_column[self.FAMILY_BROWSERS]

    def set_screen_res(self, value):
        family_column = self._get_column_family(self.FAMILY_STAT)
        family_column[self.FAMILY_SCREEN_RESOLUTIONS] = value

    def get_screen_res(self):
        family_column = self._get_column_family(self.FAMILY_STAT)
        if self.FAMILY_SCREEN_RESOLUTIONS not in family_column:
            family_column[self.FAMILY_SCREEN_RESOLUTIONS] = dict()
        return family_column[self.FAMILY_SCREEN_RESOLUTIONS]

    def set_languages(self, value):
        family_column = self._get_column_family(self.FAMILY_STAT)
        family_column[self.FAMILY_LANGUAGES] = value

    def get_languages(self):
        family_column = self._get_column_family(self.FAMILY_STAT)
        if self.FAMILY_LANGUAGES not in family_column:
            family_column[self.FAMILY_LANGUAGES] = dict()
        return family_column[self.FAMILY_LANGUAGES]

    def set_countries(self, value):
        family_column = self._get_column_family(self.FAMILY_STAT)
        family_column[self.FAMILY_COUNTRIES] = value

    def get_countries(self):
        family_column = self._get_column_family(self.FAMILY_STAT)
        if self.FAMILY_COUNTRIES not in family_column:
            family_column[self.FAMILY_COUNTRIES] = dict()
        return family_column[self.FAMILY_COUNTRIES]
