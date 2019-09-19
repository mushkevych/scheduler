__author__ = 'Bohdan Mushkevych'

import calendar
import collections

from synergy.system import time_helper
from synergy.system.time_qualifier import *


class TimeperiodDict(collections.MutableMapping):
    """ module represents a _smart_ dictionary, where key is a timeperiod
    TimeperiodDict allows timeperiod grouping, such as *every 3 hours* or *quarterly* or *weekly* """

    def __init__(self, time_qualifier, time_grouping, *args, **kwargs):
        assert time_qualifier in [QUALIFIER_HOURLY, QUALIFIER_DAILY, QUALIFIER_MONTHLY, QUALIFIER_YEARLY], \
            f'time qualifier {time_qualifier} is not supported by TimeperiodDict'
        super(TimeperiodDict, self).__init__()

        self.time_grouping = time_grouping
        self.time_qualifier = time_qualifier

        # validation section
        upper_boundary = self._get_stem_upper_boundary()
        assert 1 <= time_grouping <= upper_boundary

        # format: {grouped_timeperiod: value}
        self.data = dict()
        self.update(dict(*args, **kwargs))

    def _get_stem_upper_boundary(self, timeperiod=None):
        """
        :param timeperiod: optional parameter, applicable for QUALIFIER_DAILY qualifier only
        :return: upper boundary for dictionary's time_qualifier
        """

        if self.time_qualifier == QUALIFIER_HOURLY:
            upper_boundary = 23
        elif self.time_qualifier == QUALIFIER_DAILY:
            if timeperiod:
                # DAILY upper boundary is month-dependent
                # i.e. it is 28 for Feb 2015; and 31 for Mar 2015
                year, month, day, hour = time_helper.tokenize_timeperiod(timeperiod)
                monthrange_tuple = calendar.monthrange(int(year), int(month))
                upper_boundary = monthrange_tuple[1]
            else:
                upper_boundary = 28
        elif self.time_qualifier == QUALIFIER_MONTHLY:
            upper_boundary = 12
        elif self.time_qualifier == QUALIFIER_YEARLY:
            upper_boundary = 1
        else:
            raise ValueError(f'unknown time qualifier: {self.time_qualifier}')
        return upper_boundary

    def _do_stem_grouping(self, timeperiod, stem):
        """
        method performs *timeperiod's stem grouping*.
        :param timeperiod: timeperiod to augment
        :param stem: inclusive lower boundary for the timeperiod's corresponding token. For instance:
                - for 2015010520 and QUALIFIER_MONTHLY, stem would be 01
                - for 2015010520 and QUALIFIER_DAILY, stem would be 05
                - for 2015010520 and QUALIFIER_HOURLY, stem would be 20
        :return: grouped stem. For instance:
                - for 2015010520 and QUALIFIER_MONTHLY and time_grouping=3, stem would be 03
                - for 2015010520 and QUALIFIER_DAILY and time_grouping=2, stem would be 06
                - for 2015010520 and QUALIFIER_HOURLY and time_grouping=8, stem would be 23
        """

        # exclude 00 from lower boundary, unless the grouping == 1
        lower_boundary = 0 if self.time_grouping == 1 else 1
        upper_boundary = self._get_stem_upper_boundary(timeperiod)

        for i in range(lower_boundary, upper_boundary):
            candidate = i * self.time_grouping
            if stem <= candidate <= upper_boundary:
                return candidate
        return upper_boundary

    def _translate_timeperiod(self, timeperiod):
        """ method translates given timeperiod to the grouped timeperiod """
        if self.time_grouping == 1:
            # no translation is performed for identity grouping
            return timeperiod

        # step 1: tokenize timeperiod into: (year, month, day, hour)
        # for instance: daily 2015031400 -> ('2015', '03', '14', '00')
        year, month, day, hour = time_helper.tokenize_timeperiod(timeperiod)

        # step 2: perform grouping on the stem
        # ex1: stem of 14 with grouping 20 -> 20
        # ex2: stem of 21 with grouping 20 -> 23
        if self.time_qualifier == QUALIFIER_HOURLY:
            stem = self._do_stem_grouping(timeperiod, int(hour))
            result = '{0}{1}{2}{3:02d}'.format(year, month, day, stem)
        elif self.time_qualifier == QUALIFIER_DAILY:
            stem = self._do_stem_grouping(timeperiod, int(day))
            result = '{0}{1}{2:02d}{3}'.format(year, month, stem, hour)
        else:  # self.time_qualifier == QUALIFIER_MONTHLY:
            stem = self._do_stem_grouping(timeperiod, int(month))
            result = '{0}{1:02d}{2}{3}'.format(year, stem, day, hour)
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
