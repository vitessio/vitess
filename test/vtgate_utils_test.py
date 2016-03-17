#!/usr/bin/env python
# coding: utf-8

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
    self.assertEquals(
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
    self.assertEquals(
        len(fake_conn.invoked_intervals), vtgate_utils.NUM_RETRIES + 1)

  def test_no_retries_inside_txn(self):
    fake_conn = FakeVtGateConnection()
    fake_conn.session = object()
    with self.assertRaises(SomeException):
      fake_conn.method(SomeException('an exception'))
    self.assertEquals(len(fake_conn.invoked_intervals), 1)

  def test_no_retries_for_non_retryable_exception(self):
    fake_conn = FakeVtGateConnection()
    with self.assertRaises(exceptions.Exception):
      fake_conn.method(exceptions.Exception('an exception'))
    self.assertEquals(len(fake_conn.invoked_intervals), 1)

  def test_no_retries_for_no_exception(self):
    fake_conn = FakeVtGateConnection()
    fake_conn.method(None)
    self.assertEquals(len(fake_conn.invoked_intervals), 1)


if __name__ == '__main__':
  utils.main()
