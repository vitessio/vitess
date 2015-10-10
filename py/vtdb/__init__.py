# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# PEP 249 complient db api for Vitess

apilevel = '2.0'
# Threads may not share the module because multi_client is not thread safe.
threadsafety = 0
paramstyle = 'named'

# DEPRECATED: Callers should use dbexceptions.%(ErrorClass)s directly.
from google3.third_party.golang.vitess.py.vtdb.dbexceptions import DatabaseError
from google3.third_party.golang.vitess.py.vtdb.dbexceptions import IntegrityError
from google3.third_party.golang.vitess.py.vtdb.dbexceptions import OperationalError
