#!/usr/bin/env python
# coding: utf-8

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


"""Tests for vtgate_utils."""

import exceptions
import time
import unittest
import utils

from vtdb import vtgate_utils


def setUpModule():
  pass


def tearDownModule():
  pass


class SomeException(exceptions.Exception):
  pass


class AnotherException(exceptions.Exception):
  pass


class FakeVtGateConnection(object):

  def __init__(self):
    self.invoked_intervals = []
    # session is used by exponential_backoff_retry
    self.session = None

  @vtgate_utils.exponential_backoff_retry(
      retry_exceptions=(SomeException, AnotherException))
  def method(self, exc_to_raise):
    self.invoked_intervals.append(int(time.time() * 1000))
    if exc_to_raise:

      raise exc_to_raise


class TestVtgateUtils(unittest.TestCase):

  def test_retry_exception(self):
    fake_conn = FakeVtGateConnection()
    with self.assertRaises(SomeException):
      fake_conn.method(SomeException('an exception'))
    self.assertEqual(
        len(fake_conn.invoked_intervals), vtgate_utils.NUM_RETRIES + 1)
    previous = fake_conn.invoked_intervals[0]
    delay = vtgate_utils.INITIAL_DELAY_MS
    for interval in fake_conn.invoked_intervals[1:]:
      self.assertTrue(interval - previous >= delay)
      previous = interval
      delay *= vtgate_utils.BACKOFF_MULTIPLIER

  def test_retry_another_exception(self):
    fake_conn = FakeVtGateConnection()
    with self.assertRaises(AnotherException):
      fake_conn.method(AnotherException('an exception'))
    self.assertEqual(
        len(fake_conn.invoked_intervals), vtgate_utils.NUM_RETRIES + 1)

  def test_no_retries_inside_txn(self):
    fake_conn = FakeVtGateConnection()
    fake_conn.session = object()
    with self.assertRaises(SomeException):
      fake_conn.method(SomeException('an exception'))
    self.assertEqual(len(fake_conn.invoked_intervals), 1)

  def test_no_retries_for_non_retryable_exception(self):
    fake_conn = FakeVtGateConnection()
    with self.assertRaises(exceptions.Exception):
      fake_conn.method(exceptions.Exception('an exception'))
    self.assertEqual(len(fake_conn.invoked_intervals), 1)

  def test_no_retries_for_no_exception(self):
    fake_conn = FakeVtGateConnection()
    fake_conn.method(None)
    self.assertEqual(len(fake_conn.invoked_intervals), 1)


if __name__ == '__main__':
  utils.main()
