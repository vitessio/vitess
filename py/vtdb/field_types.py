# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from array import array
import datetime
from decimal import Decimal
from vtdb import times

# These numbers should exactly match values defined in
# dist/mysql-5.1.52/include/mysql/mysql_com.h
VT_DECIMAL = 0
VT_TINY = 1
VT_SHORT = 2
VT_LONG = 3
VT_FLOAT = 4
VT_DOUBLE = 5
VT_NULL = 6
VT_TIMESTAMP = 7
VT_LONGLONG = 8
VT_INT24 = 9
VT_DATE = 10
VT_TIME = 11
VT_DATETIME = 12
VT_YEAR = 13
VT_NEWDATE = 14
VT_BIT = 16
VT_NEWDECIMAL = 246
VT_ENUM = 247
VT_SET = 248
VT_TINY_BLOB = 249
VT_MEDIUM_BLOB = 250
VT_LONG_BLOB = 251
VT_BLOB = 252
VT_VAR_STRING = 253
VT_STRING = 254
VT_GEOMETRY = 255


class DBAPITypeObject(object):

  def __init__(self, *values):
    self.values = values

  def __cmp__(self, other):
    if other in self.values:
      return 0
    return 1


# FIXME(msolomon) why do we have these values if they aren't referenced?
STRING = DBAPITypeObject(VT_ENUM, VT_VAR_STRING, VT_STRING)
BINARY = DBAPITypeObject(VT_TINY_BLOB, VT_MEDIUM_BLOB, VT_LONG_BLOB, VT_BLOB)
NUMBER = DBAPITypeObject(
    VT_DECIMAL, VT_TINY, VT_SHORT, VT_LONG, VT_FLOAT, VT_DOUBLE, VT_LONGLONG,
    VT_INT24, VT_YEAR, VT_NEWDECIMAL)
DATETIME = DBAPITypeObject(
    VT_TIMESTAMP, VT_DATE, VT_TIME, VT_DATETIME, VT_NEWDATE)
ROWID = DBAPITypeObject()

conversions = {
    VT_DECIMAL: Decimal,
    VT_TINY: int,
    VT_SHORT: int,
    VT_LONG: int,
    VT_FLOAT: float,
    VT_DOUBLE: float,
    VT_TIMESTAMP: times.DateTimeOrNone,
    VT_LONGLONG: int,
    VT_INT24: int,
    VT_DATE: times.DateOrNone,
    VT_TIME: times.TimeDeltaOrNone,
    VT_DATETIME: times.DateTimeOrNone,
    VT_YEAR: int,
    VT_NEWDATE: times.DateOrNone,
    VT_NEWDECIMAL: Decimal,
}


# This is a temporary workaround till we figure out how to support
# native lists in our API.
class List(list):
  pass

NoneType = type(None)

# FIXME(msolomon) we could make a SqlLiteral ABC and just type check.
# That doesn't seem dramatically better than __sql_literal__ but it might
# be move self-documenting.


def convert_bind_vars(bind_variables):
  new_vars = {}
  if bind_variables is None:
    return new_vars
  for key, val in bind_variables.items():
    if hasattr(val, '__sql_literal__'):
      new_vars[key] = val.__sql_literal__()
    elif isinstance(val, datetime.datetime):
      new_vars[key] = times.DateTimeToString(val)
    elif isinstance(val, datetime.date):
      new_vars[key] = times.DateToString(val)
    elif isinstance(val, set):
      new_vars[key] = sorted(val)
    elif isinstance(val, tuple):
      new_vars[key] = list(val)
    elif isinstance(val, (int, float, str, list, NoneType)):
      new_vars[key] = val
    else:
      # NOTE(msolomon) begrudgingly I allow this - we just have too much code
      # that relies on this.
      # This accidentally solves our hideous dependency on mx.DateTime.
      new_vars[key] = str(val)
  return new_vars
