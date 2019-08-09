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

"""Define abstractions for various mysql flavors.

This module is used by mysql_db_mysqlctl.py to handle differences
between various flavors of mysql.
"""

import logging
import os
import sys


# For now, vttop is only used in this module. If other people
# need this, we should move it to environment.
if "VTTOP" not in os.environ:
  sys.stderr.write(
      "ERROR: Vitess environment not set up. "
      'Please run "source dev.env" first.\n')
  sys.exit(1)

# vttop is the toplevel of the vitess source tree
vttop = os.environ["VTTOP"]


class MysqlFlavor(object):
  """Base class with default SQL statements."""

  def my_cnf(self):
    """Returns the path to an extra my_cnf file, or None."""
    return None


class MariaDB(MysqlFlavor):
  """Overrides specific to MariaDB."""

  def my_cnf(self):
    files = [
        os.path.join(vttop, "config/mycnf/testsuite.cnf"),
    ]
    return ":".join(files)

class MariaDB103(MysqlFlavor):
  """Overrides specific to MariaDB 10.3"""

  def my_cnf(self):
    files = [
      os.path.join(vttop, "config/mycnf/testsuite.cnf"),
    ]
    return ":".join(files)

class MySQL56(MysqlFlavor):
  """Overrides specific to MySQL 5.6."""

  def my_cnf(self):
    files = [
        os.path.join(vttop, "config/mycnf/testsuite.cnf"),
    ]
    return ":".join(files)

class MySQL80(MysqlFlavor):
  """Overrides specific to MySQL 8.0."""

  def my_cnf(self):
    files = [
        os.path.join(vttop, "config/mycnf/testsuite.cnf"),
    ]
    return ":".join(files)

__mysql_flavor = None


# mysql_flavor is a function because we need something to import before the
# actual __mysql_flavor is initialized, since that doesn't happen until after
# the command-line options are parsed. If we make mysql_flavor a variable and
# import it before it's initialized, the module that imported it won't get the
# updated value when it's later initialized.
def mysql_flavor():
  return __mysql_flavor


def set_mysql_flavor(flavor):
  global __mysql_flavor

  # Last default is there because the environment variable might be set to "".
  flavor = flavor or os.environ.get("MYSQL_FLAVOR", "MariaDB") or "MariaDB"

  # Set the environment variable explicitly in case we're overriding it via
  # command-line flag.
  os.environ["MYSQL_FLAVOR"] = flavor

  if flavor == "MariaDB":
    __mysql_flavor = MariaDB()
  elif flavor == "MariaDB103":
    __mysql_flavor = MariaDB103()
  elif flavor == "MySQL80":
    __mysql_flavor = MySQL80()
  elif flavor == "MySQL56":
    __mysql_flavor = MySQL56()
  else:
    logging.error("Unknown MYSQL_FLAVOR '%s'", flavor)
    exit(1)

  logging.debug("Using MYSQL_FLAVOR=%s", str(flavor))
