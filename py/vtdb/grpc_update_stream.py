# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from itertools import izip
import logging
from urlparse import urlparse

from vtdb import dbexceptions
from vtdb import field_types
from vtdb import update_stream

from vtproto import binlogdata_pb2
from vtproto import binlogservice_pb2
from vtproto import replicationdata_pb2

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
  """GRPCUpdateStreamConnection is the gRPC implementation of
  UpdateStreamConnection.
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
    self.stub = binlogservice_pb2.early_adopter_create_UpdateStream_stub(
        p.hostname, p.port)

  def close(self):
    self.stub = None

  def is_closed(self):
    return self.stub == None

  def stream_update(self, position, timeout=3600.0):
    req = binlogdata_pb2.StreamUpdateRequest(position=position)

    with self.stub as stub:
      it = stub.StreamUpdate(req, timeout)
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

        try:
          yield update_stream.StreamEvent(category=int(stream_event.category),
                                          table_name=stream_event.table_name,
                                          fields=fields,
                                          rows=rows,
                                          sql=stream_event.sql,
                                          timestamp=stream_event.timestamp,
                                          position=stream_event.position)
        except GeneratorExit:
          # if the loop is interrupted for any reason, we need to
          # cancel the iterator, so we close the RPC connection,
          # and the with __exit__ statement is executed.

          # FIXME(alainjobart) this is flaky. It sometimes doesn't stop
          # the iterator, and we don't get out of the 'with'.
          # Sending a Ctrl-C to the process then works for some reason.
          it.cancel()
          break

update_stream.register_conn_class('grpc', GRPCUpdateStreamConnection)
