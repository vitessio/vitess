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

"""This module defines a mysqlctl based MySQL database.
"""

import os
import subprocess

import MySQLdb

from vttest import environment
from vttest import mysql_db
from vttest.mysql_flavor import mysql_flavor


class MySqlDBMysqlctl(mysql_db.MySqlDB):
  """Contains data and methods to manage a MySQL instance using mysqlctl."""

  def __init__(self, directory, port, extra_my_cnf, snapshot_file=None):
    super(MySqlDBMysqlctl, self).__init__(
        directory, port, extra_my_cnf, snapshot_file)

  def setup(self):
    cmd = [
        environment.mysqlctl_binary,
        '-alsologtostderr',
        '-tablet_uid', '1',
        '-mysql_port', str(self._port),
        '-db-config-dba-charset', 'utf8',
        '-db-config-dba-uname', 'vt_dba',
        'init',
        '-init_db_sql_file',
        os.path.join(os.environ['VTTOP'], 'config/init_db.sql'),
    ]
    env = os.environ
    env['VTDATAROOT'] = self._directory
    my_cnf = mysql_flavor().my_cnf()
    if self._extra_my_cnf:
      my_cnf += ':%s' % self._extra_my_cnf
    env['EXTRA_MY_CNF'] = my_cnf
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

  def config(self):
    return {
        'username': self.username(),
        'password': self.password(),
        'socket': self.unix_socket(),
    }
