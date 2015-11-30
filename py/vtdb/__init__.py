"""A mostly deprecated module."""

# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from vtdb import vtgatev2

# PEP 249 complient db api for Vitess

apilevel = '2.0'
# Threads may not share the module because multi_client is not thread safe.
threadsafety = 0
paramstyle = 'named'

_vtgate_client_registered_conn_class_modules = [vtgatev2]
