#!/usr/bin/env python
# coding: utf-8

"""Tests for vtgate_utils."""

import exceptions
import time
import unittest
import utils

from net import gorpc
from vtdb import vtgate_utils
from vtdb import vtgatev2
from vtproto import vtrpc_pb2


def setUpModule():
  pass


def tearDownModule():
  pass


class SomeException(exceptions.Exception):
  pass


class AnotherException(exceptions.Exception):
  pass


class FakeVtGateConnection(vtgatev2.VTGateConnection):

  def __init__(self):
    self.invoked_intervals = []
    self.keyspace = 'test_keyspace'
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


class TestExtractRPCError(unittest.TestCase):
  """Tests extract_rpc_error is tolerant to various responses."""

  def test_reply_is_none(self):
    vtgate_utils.extract_rpc_error('method', gorpc.GoRpcResponse())

  def test_reply_is_empty_string(self):
    response = gorpc.GoRpcResponse()
    vtgate_utils.extract_rpc_error('method', response)

  def test_reply_is_string(self):
    response = gorpc.GoRpcResponse()
    response.reply = 'foo'
    vtgate_utils.extract_rpc_error('method', response)

  def test_reply_is_dict(self):
    response = gorpc.GoRpcResponse()
    response.reply = {'foo': 'bar'}
    vtgate_utils.extract_rpc_error('method', response)

  def test_reply_has_non_dict_err(self):
    response = gorpc.GoRpcResponse()
    response.reply = {'Err': 1}
    with self.assertRaisesRegexp(vtgate_utils.VitessError, 'UNKNOWN_ERROR'):
      vtgate_utils.extract_rpc_error('method', response)

  def test_reply_has_missing_err_message(self):
    response = gorpc.GoRpcResponse()
    response.reply = {'Err': {'foo': 'bar'}}
    with self.assertRaisesRegexp(vtgate_utils.VitessError,
        'Missing error message'):
      vtgate_utils.extract_rpc_error('method', response)

  def test_reply_has_err_message(self):
    response = gorpc.GoRpcResponse()
    response.reply = {'Err': {'Message': 'bar'}}
    with self.assertRaisesRegexp(vtgate_utils.VitessError,
        'UNKNOWN_ERROR.+bar'):
      vtgate_utils.extract_rpc_error('method', response)

  def test_reply_has_err_code(self):
    response = gorpc.GoRpcResponse()
    response.reply = {'Err': {'Code': vtrpc_pb2.TRANSIENT_ERROR}}
    with self.assertRaisesRegexp(vtgate_utils.VitessError,
        'TRANSIENT_ERROR'):
      vtgate_utils.extract_rpc_error('method', response)

if __name__ == '__main__':
  utils.main()
