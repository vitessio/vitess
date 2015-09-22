# Copyright 2015 Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""This module defines the interface for the MySQL database.
"""


class MySqlDB(object):
  """A MySqlDB contains basic info about a MySQL instance."""

  def __init__(self, directory, port):
    self._directory = directory
    self._port = port

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
