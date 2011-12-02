"""
Created on 2011-01-28

@author: Bohdan Mushkevych
"""

# pylint: disable=W0511
class AbstractCollection(object):
    """
    This class presents common functionality for all Synergy Collections
    """
    # FIELDS
    DOMAIN_NAME = 'domain_name'      #'key'
    TIMESTAMP = 'timestamp'      #'t'
    TIMEPERIOD = 'timeperiod'      #'t'
    CREATION_TIME = 'creation_time'              #'created_at'
    SESSION_ID = 'session_id'    #'ses_id'
    IP        = 'ip'             #'ip'
    OS        = 'os'             #'os'
    BROWSER   = 'browser'        #'brws'
    USER_ID   = 'user_id'        #'usr'
    SCREEN_RESOLUTION_X = 'screen_resolution_x'   #'res_x'
    SCREEN_RESOLUTION_Y = 'screen_resolution_y'   #'res_y'
    LANGUAGE  = 'language'       #'lng'
    COUNTRY   = 'country'        #'cntr'
    PAGE      = 'page'           #'pg'

    TOTAL_DURATION = 'total_duration'                 #'t_dur'
    NUMBER_OF_UNIQUE_VISITORS = 'number_of_uniques'   #'n_unq'
    NUMBER_OF_VISITS = 'number_of_visits'             #'n_vst'
    NUMBER_OF_ENTRIES = 'number_of_entries'           #'n_ent'
    NUMBER_OF_PAGEVIEWS = 'number_of_pageviews'       #'n_pv'

    # FAMILIES
    FAMILY_STAT = 'stat'
    FAMILY_SITES = 'site'
    FAMILY_USER_PROFILE = 'user_profile'              #'usr_prfl'
    FAMILY_SITE_PROFILE = 'site_profile'              #'s_prfl'
    FAMILY_BROWSING_HISTORY = 'browsing_history'      #'brws_hst'
    FAMILY_ENTRIES = 'entries'                        #'ents'
    FAMILY_VISITS = 'visit'                           #'vsts'
    FAMILY_PAGEVIEWS = 'pageview'
    FAMILY_DURATION = 'duration'
    FAMILY_SESSIONS = 'session'                       #'sess'
    FAMILY_COUNTRIES = 'country'                      #'cntrs'
    FAMILY_OS = 'os'                                  #'oss'
    FAMILY_BROWSERS = 'browser'                       #'brws'
    FAMILY_SCREEN_RESOLUTIONS = 'screen_resolution'   #'scr_ress'
    FAMILY_LANGUAGES = 'language'                     #'lngs'

    def __init__(self, document = None):
        if document is None:
            self.data = dict()
        else:
            self.data = document
    
    def get_key(self):
        return self.data[self.DOMAIN_NAME], self.data[self.TIMESTAMP]

    def composite_key(self, key, timestamp):
        """
        @param key: id of the template
        @param timestamp: as string in Synergy Data format
        """
        self.data[self.DOMAIN_NAME] = key
        self.data[self.TIMESTAMP] = timestamp

    def _get_column_family(self, family_name):
        if family_name not in self.data:
            self.data[family_name] = dict()
        return self.data[family_name]
        
    def get_document(self):
        return self.data

    @classmethod
    def _increment_family_property(cls, key, family):
        if key is None:
            return
        
        if not isinstance(key, basestring):
            key = str(key)
        
        if key in family:
            family[key] += 1
        else:
            family[key] = 1

    @classmethod
    def _copy_and_sum_families(cls, family_source, family_target):
        """ methods iterates thru source family and copies its entries to target family
        in case key already exists in both families - then the values are added"""
        for every in family_source:
            if every not in family_target:
                family_target[every] = family_source[every]
            else:
                family_target[every] += family_source[every]
