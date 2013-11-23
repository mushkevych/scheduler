__author__ = 'Bohdan Mushkevych'

import ds_manager

KEY = 'domain_name'
TIMEPERIOD = 'timeperiod'


class BaseModel(object):
    """
    This class presents common functionality for all Models within the Scheduler project
    """

    def __init__(self, document=None):
        self._ds_manager = ds_manager.ds_factory(None)
        if document is None:
            self.data = dict()
        else:
            self.data = document

    @property
    def key(self):
        return self.data[KEY], self.data[TIMEPERIOD]

    @key.setter
    def key(self, value):
        """
        @param value: tuple - value[0] id of the template
        value[1] - timeperiod as string in Synergy Data format
        """
        self.data[KEY] = value[0]
        self.data[TIMEPERIOD] = value[1]

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
