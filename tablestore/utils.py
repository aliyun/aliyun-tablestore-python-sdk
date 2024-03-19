# -*- coding: utf8 -*-

import json
import datetime
from enum import Enum

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.name
        elif isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        elif isinstance(obj, object):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)

class DefaultJsonObject(object):
    def __repr__(self):
        return json.dumps(self, cls=MyEncoder, indent=2)
