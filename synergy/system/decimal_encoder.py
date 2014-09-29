"""
Decimal Encoder enhance regular JSON package with Decimal support
@author: Michal Marczyk
http://stackoverflow.com/questions/1960516/python-json-serialize-a-decimal-object
"""

import decimal
import json


class DecimalEncoder(json.JSONEncoder):
    def _iterencode(self, o, markers=None):
        if isinstance(o, decimal.Decimal):
            # wanted a simple yield str(o) in the next line,
            # but that would mean a yield on the line with super(...),
            # which wouldn't work (see my comment below), so...
            return (str(o) for o in [o])
        return super(DecimalEncoder, self)._iterencode(o, markers)
