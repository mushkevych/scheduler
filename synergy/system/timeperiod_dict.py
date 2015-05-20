__author__ = 'Bohdan Mushkevych'

import collections
from synergy.system import time_helper
from synergy.system.time_qualifier import *


class TimeperiodDict(collections.MutableMapping):
    def __init__(self, time_qualifier, grouping=1, *args, **kwargs):
        assert time_qualifier in [QUALIFIER_HOURLY, QUALIFIER_DAILY, QUALIFIER_MONTHLY, QUALIFIER_YEARLY]
        super(TimeperiodDict, self).__init__()

        # validation section
        if time_qualifier == QUALIFIER_HOURLY:
            self.upper_boundary = 23
        elif time_qualifier == QUALIFIER_DAILY:
            self.upper_boundary = 28
        elif time_qualifier == QUALIFIER_MONTHLY:
            self.upper_boundary = 12
        elif time_qualifier == QUALIFIER_YEARLY:
            self.upper_boundary = 1
        else:
            raise ValueError('unknown time qualifier: {0}'.format(time_qualifier))
        assert 1 <= grouping <= self.upper_boundary

        self.grouping = grouping
        self.time_qualifier = time_qualifier

        self.data = dict()
        self.update(dict(*args, **kwargs))

    def _do_stem_grouping(self, stem):
        for i in range(1, self.upper_boundary):
            candidate = i * self.grouping
            if stem <= candidate <= self.upper_boundary:
                return candidate
        return self.upper_boundary

    def _translate_timeperiod(self, timeperiod):
        """ method translates given timeperiod to the grouped timeperiod """
        if self.time_qualifier == QUALIFIER_YEARLY:
            # YEARLY timeperiods are allowed to have only identity grouping
            return timeperiod

        # step 1: tokenize timeperiod into: prefix, stem, suffix
        # for instance: daily 2015031400 -> [201503, 14, 00]
        prefix, stem, suffix = time_helper.tokenize_timeperiod(self.time_qualifier, timeperiod)

        # step 2: perform grouping on the stem
        # ex1: stem of 14 with grouping 20 -> 20
        # ex2: stem of 21 with grouping 20 -> 23
        stem = int(stem)
        grouped = self._do_stem_grouping(stem)

        # step 3: concatenate timeperiod stem + grouped timeperiod + trailing zeros
        # for instance: [201503, 20, 00] -> 2015032000
        return prefix + str(grouped) + suffix

    def __len__(self):
        return len(self.data)

    def __getitem__(self, key):
        grouped_timeperiod = self._translate_timeperiod(key)
        return self.data[grouped_timeperiod]

    def __setitem__(self, key, value):
        grouped_timeperiod = self._translate_timeperiod(key)
        self.data[grouped_timeperiod] = value

    def __delitem__(self, key):
        grouped_timeperiod = self._translate_timeperiod(key)
        del self.data[grouped_timeperiod]

    def __iter__(self):
        return iter(self.data)

    def __contains__(self, key):
        grouped_timeperiod = self._translate_timeperiod(key)
        return grouped_timeperiod in self.data
