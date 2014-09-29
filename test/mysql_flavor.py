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


class GoogleMysql(MysqlFlavor):
  """Overrides specific to Google MySQL"""

  def extra_my_cnf(self):
    return environment.vttop + "/config/mycnf/master_google.cnf"

  def master_position(self, tablet):
    conn, cursor = tablet.connect_dict("")
    try:
      cursor.execute("SHOW MASTER STATUS")
      group_id = cursor.fetchall()[0]["Group_ID"]

      cursor.execute("SHOW BINLOG INFO FOR " + str(group_id))
      server_id = cursor.fetchall()[0]["Server_ID"]

      return {"GoogleMysql": "%u-%u" % (server_id, group_id)}
    finally:
      conn.close()

  def position_equal(self, a, b):
    return int(a["GoogleMysql"].split("-")[1]) == int(
        b["GoogleMysql"].split("-")[1])

  def position_at_least(self, a, b):
    return int(a["GoogleMysql"].split("-")[1]) >= int(
        b["GoogleMysql"].split("-")[1])

  def change_master_commands(self, host, port, pos):
    parts = pos["GoogleMysql"].split("-")
    server_id = parts[0]
    group_id = parts[1]
    return [
        "SET binlog_group_id = %s, master_server_id = %s" %
        (group_id, server_id),
        "CHANGE MASTER TO "
        "MASTER_HOST='%s', MASTER_PORT=%u, CONNECT_USING_GROUP_ID" %
        (host, port)]


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
        "MASTER_HOST='%s', MASTER_PORT=%u, MASTER_USE_GTID = slave_pos" %
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
  global __mysql_flavor

  if not flavor:
    flavor = os.environ.get("MYSQL_FLAVOR", "GoogleMysql")
    # The environment variable might be set, but equal to "".
    if flavor == "":
      flavor = "GoogleMysql"

  # Don't pass the env var to subprocesses. Let them auto-detect the flavor.
  os.environ.pop("MYSQL_FLAVOR", None)

  if flavor == "MariaDB":
    __mysql_flavor = MariaDB()
  elif flavor == "GoogleMysql":
    __mysql_flavor = GoogleMysql()
  else:
    logging.error("Unknown MYSQL_FLAVOR '%s'", flavor)
    exit(1)

  logging.debug("Using MYSQL_FLAVOR=%s", str(flavor))
