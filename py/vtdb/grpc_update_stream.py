# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import logging

from itertools import izip
from urlparse import urlparse

from grpc.beta import implementations
from grpc.framework.interfaces.face import face

from vtproto import binlogdata_pb2
from vtproto import binlogservice_pb2

from vtdb import field_types
from vtdb import update_stream


def _make_row(row, conversions):
  converted_row = []
  for conversion_func, field_data in izip(conversions, row):
    if field_data is None:
      v = None
    elif conversion_func:
      v = conversion_func(field_data)
    else:
      v = field_data
    converted_row.append(v)
  return converted_row


class GRPCUpdateStreamConnection(update_stream.UpdateStreamConnection):
  """The gRPC implementation of UpdateStreamConnection.

  It is registered as 'grpc' protocol.
  """

  def __init__(self, addr, timeout):
    self.addr = addr
    self.timeout = timeout
    self.stub = None

  def __str__(self):
    return '<GRPCUpdateStreamConnection %s>' % self.addr

  def dial(self):
    logging.error("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    p = urlparse('http://' + self.addr)
    channel = implementations.insecure_channel(p.hostname, p.port)
    self.stub = binlogservice_pb2.beta_create_UpdateStream_stub(channel)

  def close(self):
    logging.error("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
    self.stub = None

  def is_closed(self):
    return self.stub is None

  def stream_update(self, position, timeout=3600.0):
    try:
      req = binlogdata_pb2.StreamUpdateRequest(position=position)

      it = self.stub.StreamUpdate(req, timeout)
      for response in it:
        stream_event = response.stream_event
        fields = []
        rows = []
        if stream_event.primary_key_fields:
          conversions = []
          for field in stream_event.primary_key_fields:
            fields.append(field.name)
            conversions.append(field_types.conversions.get(field.type))

          for r in stream_event.primary_key_values:
            row = tuple(_make_row(r.values, conversions))
            rows.append(row)

        yield update_stream.StreamEvent(
            category=int(stream_event.category),
            table_name=stream_event.table_name,
            fields=fields,
            rows=rows,
            sql=stream_event.sql,
            timestamp=stream_event.timestamp,
            transaction_id=stream_event.transaction_id)
    except face.AbortionError, e:
      # FIXME(alainjobart) These exceptions don't print well, so raise
      # one that will.  The real fix is to define a set of exceptions
      # for this library and raise that, but it's more work.
      raise Exception(e.details, e)

update_stream.register_conn_class('grpc', GRPCUpdateStreamConnection)
