#!/usr/bin/env python
"""Define abstractions for various MySQL flavors."""

import environment
import logging
import os
import subprocess


class MysqlFlavor(object):
  """Base class with default SQL statements."""

  def promote_slave_commands(self):
    """Returns commands to convert a slave to a master."""
    return [
        "STOP SLAVE",
        "RESET SLAVE",
        "RESET MASTER",
    ]

  def reset_replication_commands(self):
    """Returns commands to reset replication settings."""
    return [
        "STOP SLAVE",
        "RESET SLAVE",
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

  def position_append(self, pos, gtid):
    """Returns a new position with the given GTID appended."""
    raise NotImplementedError()

  def enable_binlog_checksum(self, tablet):
    """Enables binlog_checksum and returns True if the flavor supports it.

    Arg:
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
        "RESET SLAVE",
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

  def position_append(self, pos, gtid):
    if self.position_at_least(pos, gtid):
      return pos
    else:
      return gtid

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

  def position_append(self, pos, gtid):
    return "MySQL56/" + subprocess.check_output([
        "mysqlctl", "position", "append", pos, gtid,
    ]).strip()

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


__mysql_flavor = None


# mysql_flavor is a function because we need something to import before the
# actual __mysql_flavor is initialized, since that doesn't happen until after
# the command-line options are parsed. If we make mysql_flavor a variable and
# import it before it's initialized, the module that imported it won't get the
# updated value when it's later initialized.
def mysql_flavor():
  return __mysql_flavor


def set_mysql_flavor(flavor):
  """Set the object that will be returned by mysql_flavor().

  If flavor is not specified, set it based on MYSQL_FLAVOR environment variable.
  """

  global __mysql_flavor

  if not flavor:
    flavor = os.environ.get("MYSQL_FLAVOR", "MariaDB")
    # The environment variable might be set, but equal to "".
    if not flavor:
      flavor = "MariaDB"

  # Set the environment variable explicitly in case we're overriding it via
  # command-line flag.
  os.environ["MYSQL_FLAVOR"] = flavor

  if flavor == "MariaDB":
    __mysql_flavor = MariaDB()
  elif flavor == "MySQL56":
    __mysql_flavor = MySQL56()
  else:
    logging.error("Unknown MYSQL_FLAVOR '%s'", flavor)
    exit(1)

  logging.debug("Using MYSQL_FLAVOR=%s", str(flavor))
