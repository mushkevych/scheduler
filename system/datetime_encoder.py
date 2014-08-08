"""
Datetime Encoder enhance regular JSON package with Datetime support
@author: codingatty, Bohdan Mushkevych
http://stackoverflow.com/a/18006338/3171310
"""

import datetime
import json


class DateTimeEncoder(json.JSONEncoder):

    def __init__(self, skipkeys=False, ensure_ascii=True, check_circular=True, allow_nan=True, sort_keys=False,
                 indent=None, separators=None, encoding='utf-8', default=None, datetime_format=None):
        super(DateTimeEncoder, self).__init__(skipkeys, ensure_ascii, check_circular, allow_nan, sort_keys, indent,
                                              separators, encoding, default)
        if datetime_format is not None:
            self.datetime_format = datetime_format
        else:
            self.datetime_format = '%Y-%m-%d %H:%M:%S'

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            encoded_object = obj.strftime(self.datetime_format)
        else:
            encoded_object = json.JSONEncoder.default(self, obj)
        return encoded_object
