"""gRPC update_stream.UpdateStreamConnection implementation.
"""
from urlparse import urlparse

# Import main protobuf library first
# to work around import order issues.
import google.protobuf  # pylint: disable=unused-import

from grpc.beta import implementations
from grpc.framework.interfaces.face import face

from vtproto import query_pb2
from vtproto import queryservice_pb2

from vtdb import dbexceptions
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
    self.stub = queryservice_pb2.beta_create_Query_stub(channel)

  def close(self):
    self.stub = None

  def is_closed(self):
    return self.stub is None

  def stream_update(self, keyspace, shard, tablet_type,
                    position='', timestamp=0,
                    timeout=3600.0):
    try:
      target = query_pb2.Target(keyspace=keyspace,
                                shard=shard,
                                tablet_type=tablet_type)
      req = query_pb2.UpdateStreamRequest(target=target,
                                          position=position,
                                          timestamp=timestamp)

      for response in self.stub.UpdateStream(req, timeout):
        yield response.event
    except face.AbortionError as e:
      # FIXME(alainjobart) These exceptions don't print well, so raise
      # one that will.  The real fix is to define a set of exceptions
      # for this library and raise that, but it's more work.
      raise dbexceptions.OperationalError(e.details, e)

update_stream.register_conn_class('grpc', GRPCUpdateStreamConnection)
