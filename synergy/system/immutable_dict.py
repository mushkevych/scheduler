__author__ = 'Raymond Hettinger'
# http://stackoverflow.com/a/9997519/3171310

import collections


class ImmutableDict(collections.Mapping):
    def __init__(self, somedict):
        self._dict = dict(somedict)   # make a copy
        self._hash = None

    def __getitem__(self, key):
        return self._dict[key]

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        return iter(self._dict)

    def __hash__(self):
        if self._hash is None:
            self._hash = hash(frozenset(self._dict.items()))
        return self._hash

    def __eq__(self, other):
        return self._dict == other._dict
