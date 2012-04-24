"""
Created on 2011-01-25

@author: bmushkevych
"""
from model.abstract_model import AbstractModel

class SingleSessionStatistics(AbstractModel):
    """
    class presents statistics, gathered during the life of the session
    """

    def __init__(self, document = None):
        super(SingleSessionStatistics, self).__init__(document)
    
    def composite_key(self, domain_name, timestamp):
        """ 
        @param domain_name: name of the site (without http://)
        @param timestamp: string in YYYYMMDD_HHMMSS format
        """
        
        self.data[self.DOMAIN_NAME] = domain_name
        self.data[self.TIMESTAMP] = timestamp 
        
    def set_session_id(self, value):
        family_column = self._get_column_family(self.FAMILY_USER_PROFILE)
        family_column[self.SESSION_ID] = value
        
    def get_session_id(self):
        family_column = self._get_column_family(self.FAMILY_USER_PROFILE)
        return family_column[self.SESSION_ID]

    def set_ip(self, value):
        family_column = self._get_column_family(self.FAMILY_USER_PROFILE)
        family_column[self.IP] = value

    def get_ip(self):
        family_column = self._get_column_family(self.FAMILY_USER_PROFILE)
        return family_column[self.IP]
    
    def set_os(self, value):
        family_column = self._get_column_family(self.FAMILY_USER_PROFILE)
        family_column[self.OS] = value

    def get_os(self):
        family_column = self._get_column_family(self.FAMILY_USER_PROFILE)
        return family_column.get(self.OS)

    def set_browser(self, value):
        family_column = self._get_column_family(self.FAMILY_USER_PROFILE)
        family_column[self.BROWSER] = value

    def get_browser(self):
        family_column = self._get_column_family(self.FAMILY_USER_PROFILE)
        return family_column.get(self.BROWSER)
        
    def set_screen_res(self, value_x, value_y):
        family_column = self._get_column_family(self.FAMILY_USER_PROFILE)
        family_column[self.SCREEN_RESOLUTION_X] = value_x
        family_column[self.SCREEN_RESOLUTION_Y] = value_y

    def get_screen_res(self):
        family_column = self._get_column_family(self.FAMILY_USER_PROFILE)
        return family_column.get(self.SCREEN_RESOLUTION_X), family_column.get(self.SCREEN_RESOLUTION_Y)

    def set_language(self, value):
        family_column = self._get_column_family(self.FAMILY_USER_PROFILE)
        family_column[self.LANGUAGE] = value

    def get_language(self):
        family_column = self._get_column_family(self.FAMILY_USER_PROFILE)
        return family_column.get(self.LANGUAGE)

    def set_country(self, value):
        family_column = self._get_column_family(self.FAMILY_USER_PROFILE)
        family_column[self.COUNTRY] = value

    def get_country(self):
        family_column = self._get_column_family(self.FAMILY_USER_PROFILE)
        return family_column.get(self.COUNTRY)

    def set_total_duration(self, value):
        family_column = self._get_column_family(self.FAMILY_BROWSING_HISTORY)
        family_column[self.TOTAL_DURATION] = value

    def get_total_duration(self):
        family_column = self._get_column_family(self.FAMILY_BROWSING_HISTORY)
        if self.TOTAL_DURATION not in family_column:
            return 0
        else:
            return family_column.get(self.TOTAL_DURATION)

    def set_number_of_pageviews(self, value):
        family_column = self._get_column_family(self.FAMILY_BROWSING_HISTORY)
        family_column[self.NUMBER_OF_PAGEVIEWS] = value

    def get_number_of_pageviews(self):
        family_column = self._get_column_family(self.FAMILY_BROWSING_HISTORY)
        return family_column.get(self.NUMBER_OF_PAGEVIEWS)

    def set_number_of_entries(self, value):
        family_column = self._get_column_family(self.FAMILY_BROWSING_HISTORY)
        family_column[self.NUMBER_OF_ENTRIES] = value

    def get_number_of_entries(self):
        family_column = self._get_column_family(self.FAMILY_BROWSING_HISTORY)
        return family_column.get(self.NUMBER_OF_ENTRIES)

    def _get_entry(self, entry_id):
        """ entry_id is numerical index """
        browsing_history = self._get_column_family(self.FAMILY_BROWSING_HISTORY)
        if self.FAMILY_ENTRIES not in browsing_history:
            browsing_history[self.FAMILY_ENTRIES] = []
        if len(browsing_history[self.FAMILY_ENTRIES]) <= entry_id:
            for _ in range(entry_id - len(browsing_history[self.FAMILY_ENTRIES]) + 1) :
                browsing_history[self.FAMILY_ENTRIES].append(dict())
        return browsing_history[self.FAMILY_ENTRIES][entry_id]
        
    def set_entry_timestamp(self, entry_id, value):
        entry = self._get_entry(entry_id)
        entry[self.TIMESTAMP] = value
    
    def get_entry_timestamp(self, entry_id):
        entry = self._get_entry(entry_id)
        return entry[self.TIMESTAMP]

