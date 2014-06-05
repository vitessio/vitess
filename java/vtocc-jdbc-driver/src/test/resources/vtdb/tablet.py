# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""Replacing original vtdb vtocc implementation with zxJDBC-based one.

Instead of using python/vtdb/bson/vtocc we will use python/zxJDBC/Java/VtoccJdbcDriver/vtocc.
"""

__author__ = 'timofeyb'


import MySQLdb

from com.ziclix.python.sql import zxJDBC


# noinspection PyUnusedLocal
def connect(url, ignored_param1, keyspace, ignored_param2, ignored_param3):
  """Returns Python DB API connection that uses Java implementation.

  Additionally checks if there are non-executed yet MySQLdb statements and executes them.

  :Args:
    :param url: ``"host:port"`` Vtocc server address.
    :param ignored_param1: Ignored.
    :param keyspace: Keyspace to use to connect to Vtocc, use ``''`` if ``None``.
    :param ignored_param2: Ignored.
    :param ignored_param3: Ignored.

  :Returns:
    Connection implemented by JDBC driver.
  """
  # noinspection PyUnresolvedReferences
  conn = zxJDBC.connect("jdbc:vtocc://" + url + "/" + keyspace, None, None, "java.lang.Object")
  cursor = conn.cursor()
  while MySQLdb.sql_statements:
    sql = MySQLdb.sql_statements.pop()
    cursor.execute(sql, {})
  return conn
