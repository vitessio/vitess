# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from urlparse import urlparse

from grpc.beta import implementations
from grpc.framework.interfaces.face import face

from vtproto import binlogdata_pb2
from vtproto import binlogservice_pb2

from vtdb import dbexceptions
from vtdb import field_types_proto3
from vtdb import update_stream


def _make_row(row, conversions):
  """Builds a python native row from proto3 row."""
  converted_row = []
  offset = 0
  for i, l in enumerate(row.lengths):
    if l == -1:
      converted_row.append(None)
    elif conversions[i]:
      converted_row.append(conversions[i](row.values[offset:offset+l]))
      offset += l
    else:
      converted_row.append(row.values[offset:offset+l])
      offset += l
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
    p = urlparse('http://' + self.addr)
    channel = implementations.insecure_channel(p.hostname, p.port)
    self.stub = binlogservice_pb2.beta_create_UpdateStream_stub(channel)

  def close(self):
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
            conversions.append(field_types_proto3.conversions.get(field.type))

          for r in stream_event.primary_key_values:
            row = tuple(_make_row(r, conversions))
            rows.append(row)

        yield update_stream.StreamEvent(
            category=int(stream_event.category),
            table_name=stream_event.table_name,
            fields=fields,
            rows=rows,
            sql=stream_event.sql,
            timestamp=stream_event.timestamp,
            transaction_id=stream_event.transaction_id)
    except face.AbortionError as e:
      # FIXME(alainjobart) These exceptions don't print well, so raise
      # one that will.  The real fix is to define a set of exceptions
      # for this library and raise that, but it's more work.
      raise dbexceptions.OperationalError(e.details, e)

update_stream.register_conn_class('grpc', GRPCUpdateStreamConnection)
