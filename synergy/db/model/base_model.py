__author__ = 'Bohdan Mushkevych'

KEY = 'key'
TIMEPERIOD = 'timeperiod'


class BaseModel(object):
    """
    This class presents common functionality for all Models within the Synergy Scheduler project
    """

    def __init__(self, document=None):
        if document is None:
            self.data = dict()
        else:
            self.data = document

    @property
    def key(self):
        raise NotImplementedError('property key.getter is not implemented in BaseModel child %s'
                                  % self.__class__.__name__)

    @key.setter
    def key(self, value):
        raise NotImplementedError('property key.getter is not implemented in BaseModel child %s'
                                  % self.__class__.__name__)

    @property
    def db_id(self):
        return str(self.data['_id'])

    def _get_column_family(self, family_name):
        if family_name not in self.data:
            self.data[family_name] = dict()
        return self.data[family_name]

    @property
    def document(self):
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
