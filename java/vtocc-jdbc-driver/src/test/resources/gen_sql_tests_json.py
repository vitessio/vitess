"""Generator for SQL tests in a form of JSON file from Vtocc python source code.
"""

__author__ = 'timofeyb@google.com (Timothy Basanov)'

import datetime
import decimal
import json

from queryservice_tests import cases_framework
from queryservice_tests import nocache_cases


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, cases_framework.MultiCase):
            return obj.__dict__
        if isinstance(obj, cases_framework.Case):
            return obj.__dict__
        if isinstance(obj, decimal.Decimal):
            return {'decimal': str(obj)}
        if isinstance(obj, datetime.datetime):
            return {'datetime': str(obj)}
        if isinstance(obj, datetime.timedelta):
            return {'timedelta': str(obj)}
        if isinstance(obj, datetime.date):
            return {'date': str(obj)}
        return json.JSONEncoder.default(self, obj)


def main():
    print json.dumps(nocache_cases.cases, cls=Encoder, indent=2)


if __name__ == '__main__':
    main()

