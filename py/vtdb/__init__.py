"""This file provides the PEP0249 compliant variables for this module.

See https://www.python.org/dev/peps/pep-0249 for more information on these.
"""

# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# Follows the Python Database API 2.0.
apilevel = '2.0'

# Threads may share the module, but not connections.
# (we store session information in the conection now, that should be in the
# cursor but are not for historical reasons).
threadsafety = 2

# Named style, e.g. ...WHERE name=:name.
#
# Note we also provide a function in dbapi to convert from 'pyformat'
# to 'named', and prune unused bind variables in the SQL query.
#
# Also, we use an extension to bind variables to handle lists:
# Using the '::name' syntax (instead of ':name') will indicate a list bind
# variable. The type then has to be a list, set or tuple.
paramstyle = 'named'
