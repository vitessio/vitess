"""gRPC update_stream.UpdateStreamConnection implementation.
"""
from urlparse import urlparse

# Import main protobuf library first
# to work around import order issues.
import google.protobuf  # pylint: disable=unused-import

from grpc.beta import implementations
from grpc.framework.interfaces.face import face

from vtproto import binlogdata_pb2
from vtproto import binlogservice_pb2

from vtdb import dbexceptions
from vtdb import proto3_encoding
from vtdb import update_stream


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
            conversions.append(proto3_encoding.conversions.get(field.type))

          for r in stream_event.primary_key_values:
            row = tuple(proto3_encoding.make_row(r, conversions))
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
