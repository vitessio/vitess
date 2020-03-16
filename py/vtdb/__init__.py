"""This file provides the PEP0249 compliant variables for this module.

See https://www.python.org/dev/peps/pep-0249 for more information on these.
"""

# Copyright 2019 The Vitess Authors.
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

# Follows the Python Database API 2.0.
apilevel = '2.0'

# Threads may share the module, but not connections.
# (we store session information in the connection now, that should be in the
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
