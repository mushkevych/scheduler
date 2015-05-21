__author__ = 'Bohdan Mushkevych'

import calendar
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

    def _do_stem_grouping(self, timeperiod, stem):
        revisited_upper_boundary = self.upper_boundary
        if self.time_qualifier == QUALIFIER_DAILY:
            # DAILY upper boundary is month-dependent
            # i.e. it is 28 for Feb 2015; and 31 for Mar 2015
            year, month, day, hour = time_helper.tokenize_timeperiod(timeperiod)
            monthrange_tuple = calendar.monthrange(int(year), int(month))
            revisited_upper_boundary = monthrange_tuple[1]

        # exclude 00 from lower boundary, unless the grouping == 1
        revisited_lower_boundary = 0 if self.grouping == 1 else 1

        for i in range(revisited_lower_boundary, revisited_upper_boundary):
            candidate = i * self.grouping
            if stem <= candidate <= revisited_upper_boundary:
                return candidate
        return revisited_upper_boundary

    def _translate_timeperiod(self, timeperiod):
        """ method translates given timeperiod to the grouped timeperiod """
        if self.time_qualifier == QUALIFIER_YEARLY:
            # YEARLY timeperiods are allowed to have only identity grouping
            return timeperiod

        # step 1: tokenize timeperiod into: (year, month, day, hour)
        # for instance: daily 2015031400 -> ('2015', '03', '14', '00')
        year, month, day, hour = time_helper.tokenize_timeperiod(timeperiod)
        if self.time_qualifier == QUALIFIER_HOURLY:
            stem = hour
        elif self.time_qualifier == QUALIFIER_DAILY:
            stem = day
        else:  # self.time_qualifier == QUALIFIER_MONTHLY:
            stem = month

        # step 2: perform grouping on the stem
        # ex1: stem of 14 with grouping 20 -> 20
        # ex2: stem of 21 with grouping 20 -> 23
        stem = int(stem)
        grouped = self._do_stem_grouping(timeperiod, stem)

        # step 3: concatenate timeperiod components
        # for instance: ('2015', 03', '20', '00') -> 2015032000
        if self.time_qualifier == QUALIFIER_HOURLY:
            result = '{0}{1}{2}{3:02d}'.format(year, month, day, grouped)
        elif self.time_qualifier == QUALIFIER_DAILY:
            result = '{0}{1}{2:02d}{3}'.format(year, month, grouped, hour)
        else:  # self.time_qualifier == QUALIFIER_MONTHLY:
            result = '{0}{1:02d}{2}{3}'.format(year, grouped, day, hour)
        return result

    def __len__(self):
        return len(self.data)

    def __getitem__(self, key):
        grouped_timeperiod = self._translate_timeperiod(key)
        return self.data.__getitem__(grouped_timeperiod)

    def __setitem__(self, key, value):
        grouped_timeperiod = self._translate_timeperiod(key)
        self.data.__setitem__(grouped_timeperiod, value)

    def __delitem__(self, key):
        self.data.__delitem__(key)

    def __iter__(self):
        return iter(self.data)

    def __contains__(self, key):
        grouped_timeperiod = self._translate_timeperiod(key)
        return self.data.__contains__(grouped_timeperiod)
