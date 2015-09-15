"""Base bson client class."""

import itertools

from net import bsonrpc
from net import gorpc

class BsonVtgateClient(object):

  cursor_cls = None

  def __init__(self, addr, timeout, user, password, keyfile, certfile):
    self.addr = addr
    self.certfile = certfile
    self.keyfile = keyfile
    self.password = password
    self.timeout = timeout
    self.user = user
    self.client = self._create_client()

  def _create_client(self):
    return bsonrpc.BsonRpcClient(
        self.addr, self.timeout, self.user, self.password,
        keyfile=self.keyfile, certfile=self.certfile)

  def _get_client(self):
    """Get current client or create a new one and connect."""
    if not self.client:
      self.client = self._create_client()
      try:
        self.client.dial()
      except gorpc.GoRpcError as e:
        raise self.convert_gorpc_exception(e)
    return self.client

  def _get_client_for_streaming(self):
    """Get a separate client for a streaming query.

    Streaming queries may still be active while this object is doing
    other queries, so they cannot share clients.

    Returns:
      A newly opened BsonRpcClient.
    """
    streaming_client = self._create_client()
    try:
      streaming_client.dial()
    except gorpc.GoRpcError as e:
      raise self.convert_gorpc_exception(e)
    return streaming_client

  def convert_gorpc_exception(self, exc, *args, **kwargs):
    raise NotImplementedError

  def cursor(self, *pargs, **kwargs):
    cursorclass = kwargs.pop('cursorclass', None) or self.cursor_cls
    return cursorclass(self, *pargs, **kwargs)

  def dial(self):
    try:
      if not self.is_closed():
        self.close()
      self._get_client().dial()
    except gorpc.GoRpcError as e:
      raise self.convert_gorpc_exception(e)

  def in_transaction(self):
    raise NotImplementedError

  def close(self):
    if self.in_transaction():
      self.rollback()
    if self.client:
      self.client.close()

  def is_closed(self):
    return not self.client or self.client.is_closed()

  def _make_row(self, row, conversions):
    converted_row = []
    for conversion_func, field_data in itertools.izip(conversions, row):
      if field_data is None:
        v = None
      elif conversion_func:
        v = conversion_func(field_data)
      else:
        v = field_data
      converted_row.append(v)
    return converted_row
