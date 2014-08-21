#!/usr/bin/python

import environment
import logging


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

  def extra_my_cnf(self):
    """Returns the path to an extra my_cnf file, or None."""
    return None

  def bootstrap_archive(self):
    """Returns the name of the bootstrap archive for mysqlctl, relative to vitess/data/bootstrap/"""
    return "mysql-db-dir.tbz"


class GoogleMysql(MysqlFlavor):
  """Overrides specific to Google MySQL"""

  def extra_my_cnf(self):
    return environment.vttop + "/config/mycnf/master_google.cnf"


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
    return "mysql-db-dir_10.0.10-MariaDB.tbz"


if environment.mysql_flavor == "MariaDB":
  mysql_flavor = MariaDB()
elif environment.mysql_flavor == "GoogleMysql":
  mysql_flavor = GoogleMysql()
else:
  mysql_flavor = MysqlFlavor()
  logging.warning("Unknown MYSQL_FLAVOR '%s', using defaults" % environment.mysql_flavor)
