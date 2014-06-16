# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""Mocking out MySQL interactions with a fake driver.

We mocking out all MySQL interactions that create tables to be run over Vtocc
when it would be initialized. All the calls are saved into a list and are played over
Vtocc connection when it's ready.

Rationale:
Java does not support unix sockets.
MySql JDBC Driver is written in Java.
Jython can only use JDBC driver.
"""

__author__ = 'timofeyb'


sql_statements = [] # List of statements "executed" with MySql fake driver

def connect(**_):
  """Creates a fake connection."""
  return FakeConnection()


class FakeConnection(object):
  """Only fake cursor can be created."""

  def cursor(self):
    return FakeCursor()


class FakeCursor(object):
  """Logs execute() calls into static variable."""

  def execute(self, sql, _):
    sql_statements.append(sql)

  def close(self):
    """Ignored, nothing to close."""
    pass
