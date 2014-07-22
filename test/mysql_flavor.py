#!/usr/bin/python

import environment


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
    return [
        "RESET MASTER",
        "STOP SLAVE",
        "RESET SLAVE",
        'CHANGE MASTER TO MASTER_HOST = ""',
    ]


class GoogleMysql(MysqlFlavor):
  """Overrides specific to Google MySQL"""


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

if environment.mysql_flavor == "MariaDB":
  mysql_flavor = MariaDB()
else:
  mysql_flavor = GoogleMysql()
