#!/usr/bin/env python
# coding: utf-8
"""Unit tests for vtdb.tablet"""

import unittest

import mock

from net import gorpc
import utils
from vtdb import tablet


class TestRPCCallAndExtract(unittest.TestCase):
  """Tests rpc_call_and_extract_error is tolerant to various responses."""

  tablet_conn = tablet.TabletConnection(
      'addr', 'type', 'keyspace', 'shard', 30, caller_id='dev')

  def test_reply_is_none(self):
    with mock.patch.object(
        self.tablet_conn, 'client', autospec=True) as mock_client:
      mock_client.call.return_value = gorpc.GoRpcResponse()
      self.tablet_conn.rpc_call_and_extract_error('method', 'req')

  def test_reply_is_empty_string(self):
    with mock.patch.object(
        self.tablet_conn, 'client', autospec=True) as mock_client:
      response = gorpc.GoRpcResponse()
      response.reply = ''
      mock_client.call.return_value = response
      self.tablet_conn.rpc_call_and_extract_error('method', 'req')

  def test_reply_is_string(self):
    with mock.patch.object(
        self.tablet_conn, 'client', autospec=True) as mock_client:
      response = gorpc.GoRpcResponse()
      response.reply = 'foo'
      mock_client.call.return_value = response
      self.tablet_conn.rpc_call_and_extract_error('method', 'req')

  def test_reply_is_dict(self):
    with mock.patch.object(
        self.tablet_conn, 'client', autospec=True) as mock_client:
      response = gorpc.GoRpcResponse()
      response.reply = {'foo': 'bar'}
      mock_client.call.return_value = response
      self.tablet_conn.rpc_call_and_extract_error('method', 'req')

  def test_reply_has_non_dict_err(self):
    with mock.patch.object(
        self.tablet_conn, 'client', autospec=True) as mock_client:
      response = gorpc.GoRpcResponse()
      response.reply = {'Err': 'foo'}
      mock_client.call.return_value = response
      with self.assertRaisesRegexp(gorpc.AppError, 'Missing error message'):
        self.tablet_conn.rpc_call_and_extract_error('method', 'req')

  def test_reply_has_missing_err_message(self):
    with mock.patch.object(
        self.tablet_conn, 'client', autospec=True) as mock_client:
      response = gorpc.GoRpcResponse()
      response.reply = {'Err': {'foo': 'bar'}}
      mock_client.call.return_value = response
      with self.assertRaisesRegexp(gorpc.AppError, 'Missing error message'):
        self.tablet_conn.rpc_call_and_extract_error('method', 'req')

  def test_reply_has_err_message(self):
    with mock.patch.object(
        self.tablet_conn, 'client', autospec=True) as mock_client:
      response = gorpc.GoRpcResponse()
      response.reply = {'Err': {'Message': 'bar'}}
      mock_client.call.return_value = response
      with self.assertRaisesRegexp(gorpc.AppError, "'bar', 'method'"):
        self.tablet_conn.rpc_call_and_extract_error('method', 'req')

if __name__ == '__main__':
  utils.main()
