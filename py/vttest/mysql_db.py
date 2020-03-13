# Copyright 2019 The Vitess Authors.
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

"""This module defines the interface for the MySQL database.
"""


class MySqlDB(object):
  """A MySqlDB contains basic info about a MySQL instance."""

  def __init__(self, directory, port, extra_my_cnf=None, snapshot_file=None):
    self._directory = directory
    self._port = port
    self._extra_my_cnf = extra_my_cnf
    self._snapshot_file = snapshot_file

  def setup(self, port):
    """Starts the MySQL database."""
    raise NotImplementedError('MySqlDB is the base class.')

  def teardown(self):
    """Stops the MySQL database."""
    raise NotImplementedError('MySqlDB is the base class.')

  def username(self):
    raise NotImplementedError('MySqlDB is the base class.')

  def password(self):
    raise NotImplementedError('MySqlDB is the base class.')

  def hostname(self):
    raise NotImplementedError('MySqlDB is the base class.')

  def port(self):
    raise NotImplementedError('MySqlDB is the base class.')

  def unix_socket(self):
    raise NotImplementedError('MySqlDB is the base class.')

  def config(self):
    """Returns the json config to output."""
    raise NotImplementedError('MySqlDB is the base class.')
