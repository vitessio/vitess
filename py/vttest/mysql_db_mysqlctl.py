# Copyright 2015 Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""This module defines a mysqlctl based MySQL database.
"""

import os
import subprocess

import MySQLdb

from vttest import environment
from vttest import mysql_db

class MySqlDBMysqlctl(mysql_db.MySqlDB):
  """Contains data and methods to manage a MySQL instance using mysqlctl."""

  def __init__(self, directory, port):
    super(MySqlDBMysqlctl, self).__init__(directory, port)

  def setup(self):
    cmd = [
        environment.mysqlctl_binary,
        '-alsologtostderr',
        '-tablet_uid', '1',
        '-mysql_port', str(self._port),
        '-db-config-dba-charset', 'utf8',
        '-db-config-dba-uname', 'vt_dba',
        'init',
        '-bootstrap_archive', 'mysql-db-dir_10.0.13-MariaDB.tbz',
    ]
    env = os.environ
    env['VTDATAROOT'] = self._directory
    result = subprocess.call(cmd, env=env)
    if result != 0:
      raise Exception('mysqlctl failed', result)

  def teardown(self):
    cmd = [
        environment.mysqlctl_binary,
        '-alsologtostderr',
        '-tablet_uid', '1',
        '-mysql_port', str(self._port),
        '-db-config-dba-charset', 'utf8',
        '-db-config-dba-uname', 'vt_dba',
        'shutdown',
    ]
    result = subprocess.call(cmd)
    if result != 0:
      raise Exception('mysqlctl failed', result)

  def connect(self, db_name):
    return MySQLdb.connect(user='vt_dba',
                           unix_socket=self.unix_socket(),
                           db=db_name)

  def username(self):
    return 'vt_dba'

  def password(self):
    return ''

  def hostname(self):
    return ''

  def port(self):
    return self._port

  def unix_socket(self):
    return os.path.join(self._directory, 'vt_0000000001', 'mysql.sock')
