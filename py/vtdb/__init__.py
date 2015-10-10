# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# PEP 249 complient db api for Vitess

apilevel = '2.0'
# Threads may not share the module because multi_client is not thread safe.
threadsafety = 0
paramstyle = 'named'

# TODO(dumbunny): Have callers use dbexceptions.DatabaseError directly
from vtdb.dbexceptions import DatabaseError
from vtdb.dbexceptions import IntegrityError
from vtdb.dbexceptions import OperationalError
