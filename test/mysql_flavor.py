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

"""Define abstractions for various MySQL flavors."""

import environment
import logging
import os
import subprocess


class MysqlFlavor(object):
  """Base class with default SQL statements."""

  def demote_master_commands(self):
    """Returns commands to stop the current master."""
    return [
        "SET GLOBAL read_only = ON",
        "FLUSH TABLES WITH READ LOCK",
        "UNLOCK TABLES",
    ]

  def promote_slave_commands(self):
    """Returns commands to convert a slave to a master."""
    return [
        "STOP SLAVE",
        "RESET SLAVE ALL",
        "SET GLOBAL read_only = OFF",
    ]

  def reset_replication_commands(self):
    """Returns commands to reset replication settings."""
    return [
        "STOP SLAVE",
        "RESET SLAVE ALL",
        "RESET MASTER",
    ]

  def change_master_commands(self, host, port, pos):
    raise NotImplementedError()

  def set_semi_sync_enabled_commands(self, master=None, slave=None):
    """Returns commands to turn semi-sync on/off."""
    cmds = []
    if master is not None:
      cmds.append("SET GLOBAL rpl_semi_sync_master_enabled = %d" % master)
    if slave is not None:
      cmds.append("SET GLOBAL rpl_semi_sync_slave_enabled = %d" % slave)
    return cmds

  def extra_my_cnf(self):
    """Returns the path to an extra my_cnf file, or None."""
    return None

  def master_position(self, tablet):
    """Returns the position from SHOW MASTER STATUS as a string."""
    raise NotImplementedError()

  def position_equal(self, a, b):
    """Returns true if position 'a' is equal to 'b'."""
    raise NotImplementedError()

  def position_at_least(self, a, b):
    """Returns true if position 'a' is at least as far along as 'b'."""
    raise NotImplementedError()

  def position_after(self, a, b):
    """Returns true if position 'a' is after 'b'."""
    return self.position_at_least(a, b) and not self.position_equal(a, b)

  def enable_binlog_checksum(self, tablet):
    """Enables binlog_checksum and returns True if the flavor supports it.

    Args:
      tablet: A tablet.Tablet object.

    Returns:
      False if the flavor doesn't support binlog_checksum.
    """
    tablet.mquery("", "SET @@global.binlog_checksum=1")
    return True

  def disable_binlog_checksum(self, tablet):
    """Disables binlog_checksum if the flavor supports it."""
    tablet.mquery("", "SET @@global.binlog_checksum=0")


class MariaDB(MysqlFlavor):
  """Overrides specific to MariaDB."""

  def reset_replication_commands(self):
    return [
        "STOP SLAVE",
        "RESET SLAVE ALL",
        "RESET MASTER",
        "SET GLOBAL gtid_slave_pos = ''",
    ]

  def extra_my_cnf(self):
    return environment.vttop + "/config/mycnf/master_mariadb.cnf"

  def master_position(self, tablet):
    gtid = tablet.mquery("", "SELECT @@GLOBAL.gtid_binlog_pos")[0][0]
    return "MariaDB/" + gtid

  def position_equal(self, a, b):
    return a == b

  def position_at_least(self, a, b):
    # positions are MariaDB/A-B-C and we only compare C
    return int(a.split("-")[2]) >= int(b.split("-")[2])

  def change_master_commands(self, host, port, pos):
    gtid = pos.split("/")[1]
    return [
        "SET GLOBAL gtid_slave_pos = '%s'" % gtid,
        "CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, "
        "MASTER_USER='vt_repl', MASTER_USE_GTID = slave_pos" %
        (host, port)]


class MySQL56(MysqlFlavor):
  """Overrides specific to MySQL 5.6."""

  def master_position(self, tablet):
    gtid = tablet.mquery("", "SELECT @@GLOBAL.gtid_executed")[0][0]
    return "MySQL56/" + gtid

  def position_equal(self, a, b):
    return subprocess.check_output([
        "mysqlctl", "position", "equal", a, b,
    ]).strip() == "true"

  def position_at_least(self, a, b):
    return subprocess.check_output([
        "mysqlctl", "position", "at_least", a, b,
    ]).strip() == "true"

  def extra_my_cnf(self):
    return environment.vttop + "/config/mycnf/master_mysql56.cnf"

  def change_master_commands(self, host, port, pos):
    gtid = pos.split("/")[1]
    return [
        "RESET MASTER",
        "SET GLOBAL gtid_purged = '%s'" % gtid,
        "CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, "
        "MASTER_USER='vt_repl', MASTER_AUTO_POSITION = 1" %
        (host, port)]


# Map of registered MysqlFlavor classes (keyed by an identifier).
flavor_map = {}

MYSQL_FLAVOR = None


# mysql_flavor is a function because we need something to import before the
# variable MYSQL_FLAVOR is initialized, since that doesn't happen until after
# the command-line options are parsed. If we make mysql_flavor a variable and
# import it before it's initialized, the module that imported it won't get the
# updated value when it's later initialized.
def mysql_flavor():
  return MYSQL_FLAVOR


def set_mysql_flavor(flavor):
  """Set the object that will be returned by mysql_flavor().

  If flavor is not specified, set it based on MYSQL_FLAVOR environment variable.

  Args:
    flavor: String of the MySQL flavor e.g. "MariaDB" or "MySQL56".
  """

  global MYSQL_FLAVOR

  if not flavor:
    flavor = os.environ.get("MYSQL_FLAVOR", "MariaDB")
    # The environment variable might be set, but equal to "".
    if not flavor:
      flavor = "MariaDB"

  v = flavor_map.get(flavor, None)
  if not v:
    logging.error("Unknown MYSQL_FLAVOR '%s'", flavor)
    exit(1)

  cls = v["cls"]
  env = v["env"]
  MYSQL_FLAVOR = cls()
  # Set the environment variable explicitly in case we're overriding it via
  # command-line flag.
  os.environ["MYSQL_FLAVOR"] = env

  logging.debug("Using MySQL flavor: %s, setting MYSQL_FLAVOR=%s (%s)",
                str(flavor), env, cls)


def register_flavor(flavor, cls, env):
  """Register the available MySQL flavors.

  Note: We need the 'env' argument because our internal implementation is
  similar to 'MariaDB' (and hence requires MYSQL_FLAVOR=MariaDB) but has its own
  flavor class.

  Args:
    flavor: Name of the flavor (must be passed to test flag --mysql-flavor).
    cls: Class which inherits MysqlFlavor and provides the implementation.
    env: Value which will be used for the environment variable MYSQL_FLAVOR.
  """
  if flavor in flavor_map:
    old_cls = flavor_map[flavor]["cls"]
    old_env = flavor_map[flavor]["env"]
    logging.error("Cannot register MySQL flavor %s because class %s (env: %s)"
                  " is already registered for it.", flavor, old_cls, old_env)
    exit(1)

  flavor_map[flavor] = {"cls": cls, "env": env}

register_flavor("MariaDB", MariaDB, "MariaDB")
register_flavor("MySQL56", MySQL56, "MySQL56")
