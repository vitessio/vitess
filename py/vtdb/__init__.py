# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# PEP 249 complient db api for Vitess

apilevel = '2.0'
threadsafety = 0  # Threads may not share the module because multi_client is not thread safe
paramstyle = 'named'

from vtdb.dbexceptions import *
from vtdb.times import Date, Time, Timestamp, DateFromTicks, TimeFromTicks, TimestampFromTicks
from vtdb.field_types import Binary
from vtdb.field_types import STRING, BINARY, NUMBER, DATETIME, ROWID
