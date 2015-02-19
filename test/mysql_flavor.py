#!/usr/bin/env python

import environment
import logging
import os


class MysqlFlavor(object):
  """Base class with default SQL statements"""

  def promote_slave_commands(self):
    """Returns commands to convert a slave to a master."""
    return [
        "RESET MASTER",
        "STOP SLAVE",
        "RESET SLAVE",
        "CHANGE MASTER TO MASTER_HOST = ''",
    ]

  def reset_replication_commands(self):
    """Returns commands to reset replication settings."""
    return [
        "RESET MASTER",
        "STOP SLAVE",
        "RESET SLAVE",
        'CHANGE MASTER TO MASTER_HOST = ""',
    ]

  def change_master_commands(self, host, port, pos):
    return None

  def extra_my_cnf(self):
    """Returns the path to an extra my_cnf file, or None."""
    return None

  def bootstrap_archive(self):
    """Returns the name of the bootstrap archive for mysqlctl, relative to vitess/data/bootstrap/"""
    return "mysql-db-dir.tbz"

  def master_position(self, tablet):
    """Returns the position from SHOW MASTER STATUS"""
    return None

  def position_equal(self, a, b):
    """Returns true if position 'a' is equal to 'b'"""
    return None

  def position_at_least(self, a, b):
    """Returns true if position 'a' is at least as far along as 'b'."""
    return None

  def position_after(self, a, b):
    """Returns true if position 'a' is after 'b'"""
    return self.position_at_least(a, b) and not self.position_equal(a, b)

  def position_append(self, pos, gtid):
    """Returns a new position with the given GTID appended"""
    if self.position_at_least(pos, gtid):
      return pos
    else:
      return gtid

  def enable_binlog_checksum(self, tablet):
    """Enables binlog_checksum and returns True if the flavor supports it.
       Returns False if the flavor doesn't support binlog_checksum."""
    return False

  def disable_binlog_checksum(self, tablet):
    """Disables binlog_checksum if the flavor supports it."""
    return


class MariaDB(MysqlFlavor):
  """Overrides specific to MariaDB"""

  def promote_slave_commands(self):
    return [
        "RESET MASTER",
        "STOP SLAVE",
        "RESET SLAVE",
    ]

  def reset_replication_commands(self):
    return [
        "RESET MASTER",
        "STOP SLAVE",
        "RESET SLAVE",
    ]

  def extra_my_cnf(self):
    return environment.vttop + "/config/mycnf/master_mariadb.cnf"

  def bootstrap_archive(self):
    return "mysql-db-dir_10.0.13-MariaDB.tbz"

  def master_position(self, tablet):
    return {
        "MariaDB": tablet.mquery("", "SELECT @@GLOBAL.gtid_binlog_pos")[0]
                         [0]
    }

  def position_equal(self, a, b):
    return a["MariaDB"] == b["MariaDB"]

  def position_at_least(self, a, b):
    return int(a["MariaDB"].split("-")[2]) >= int(b["MariaDB"].split("-")[2])

  def change_master_commands(self, host, port, pos):
    return [
        "SET GLOBAL gtid_slave_pos = '%s'" % pos["MariaDB"],
        "CHANGE MASTER TO "
        "MASTER_HOST='%s', MASTER_PORT=%u, MASTER_USER='vt_repl', MASTER_USE_GTID = slave_pos" %
        (host, port)]

  def enable_binlog_checksum(self, tablet):
    tablet.mquery('', 'SET @@global.binlog_checksum=1')
    return True

  def disable_binlog_checksum(self, tablet):
    tablet.mquery('', 'SET @@global.binlog_checksum=0')

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

  if not flavor:
    flavor = os.environ.get("MYSQL_FLAVOR", "MariaDB")
    # The environment variable might be set, but equal to "".
    if flavor == "":
      flavor = "MariaDB"

  # Set the environment variable explicitly in case we're overriding it via
  # command-line flag.
  os.environ["MYSQL_FLAVOR"] = flavor

  if flavor == "MariaDB":
    __mysql_flavor = MariaDB()
  elif flavor == "Mysql56":
    logging.error("Mysql56 support is currently under development, and not supported yet")
    exit(1)
  else:
    logging.error("Unknown MYSQL_FLAVOR '%s'", flavor)
    exit(1)

  logging.debug("Using MYSQL_FLAVOR=%s", str(flavor))
