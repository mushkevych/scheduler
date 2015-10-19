__author__ = 'Bohdan Mushkevych'

import heapq
from datetime import datetime, timedelta

from synergy.conf import settings
from synergy.system import time_helper
from synergy.system.time_qualifier import QUALIFIER_REAL_TIME


def compute_release_time(lag_in_minutes):
    future_dt = datetime.utcnow() + timedelta(minutes=lag_in_minutes)
    release_time_str = time_helper.datetime_to_synergy(QUALIFIER_REAL_TIME, future_dt)
    return int(release_time_str)


class PriorityEntry(object):
    """ an entry for Priority Queue, where priority is *entry creation time* + *waiting time* """

    # Creation counter keeps track of PriorityEntry declaration order
    # Each time an instance is created the counter should be increased
    creation_counter = 0

    def __init__(self, entry, lag_in_minutes=settings.settings['gc_release_lag_minutes']):
        """ :param entry: the unit_of_work to reprocess """
        self.entry = entry
        self.release_time = compute_release_time(lag_in_minutes)  # SYNERGY_SESSION_PATTERN: time in the future
        self.creation_counter = PriorityEntry.creation_counter + 1
        PriorityEntry.creation_counter += 1

    def __eq__(self, other):
        return self.entry == other.entry

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        """Defines behavior for the less-than operator, <."""
        if self.release_time == other.release_time:
            return self.creation_counter < other.creation_counter
        else:
            return self.release_time < other.release_time

    def __gt__(self, other):
        """Defines behavior for the greater-than operator, >."""
        if self.release_time == other.release_time:
            return self.creation_counter > other.creation_counter
        else:
            return self.release_time > other.release_time

    def __le__(self, other):
        """Defines behavior for the less-than-or-equal-to operator, <=."""
        if self.release_time == other.release_time:
            return self.creation_counter <= other.creation_counter
        else:
            return self.release_time <= other.release_time

    def __ge__(self, other):
        """Defines behavior for the greater-than-or-equal-to operator, >=."""
        if self.release_time == other.release_time:
            return self.creation_counter >= other.creation_counter
        else:
            return self.release_time >= other.release_time

    def __hash__(self):
        return hash((self.release_time, self.creation_counter))


class PriorityQueue(object):
    """ Priority Queue that retrieves entries in the priority order (lowest first) """

    def __init__(self):
        self.queue = list()

    def __len__(self):
        return len(self.queue)

    def __contains__(self, item):
        return item in self.queue

    def put(self, item):
        heapq.heappush(self.queue, item)

    def pop(self):
        """ :return: minimal element is removed from the queue and returned to the caller """
        return heapq.heappop(self.queue)

    def peek(self):
        """ :return: minimal element without being removed from the queue """
        return min(self.queue)
