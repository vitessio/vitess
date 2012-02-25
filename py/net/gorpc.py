# Copyright 2012, Google Inc.
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:

#     * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
# copyright notice, this list of conditions and the following disclaimer
# in the documentation and/or other materials provided with the
# distribution.
#     * Neither the name of Google Inc. nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# Handle transport and serialization callbacks for Go-style RPC servers.
#
# This is pretty simple. The client initiates an HTTP CONNECT and then
# hijacks the socket. The client is synchronous, but implements deadlines.

import errno
import socket
import struct
import time
import urlparse

_len = len
_join = ''.join

class GoRpcError(Exception):
  pass

class TimeoutError(GoRpcError):
  pass

# Error field from response raised as an exception
class AppError(GoRpcError):
  pass


def make_header(method, sequence_id):
  return {'ServiceMethod': method,
          'Seq': sequence_id}


class GoRpcRequest(object):
  header = None # standard fields that route the request on the server side
  body = None # the actual request object - usually a dictionary

  def __init__(self, header, args):
    self.header = header
    self.body = args

  @property
  def sequence_id(self):
    return self.header['Seq']


class GoRpcResponse(object):
  # the request header is echoed back to detect error and out-of-sequence bugs
  # {'ServiceMethod': method,
  #  'Seq': sequence_id,
  #  'Error': error_string}
  header = None
  reply = None # the decoded object - usually a dictionary

  @property
  def error(self):
    return self.header['Error']

  @property
  def sequence_id(self):
    return self.header['Seq']


# FIXME(msolomon) This is a bson-ism, fix for future protocols.
len_struct = struct.Struct('<i')
unpack_length = len_struct.unpack_from
len_struct_size = len_struct.size
default_read_buffer_size = 8192

# A single socket wrapper to handle request/response conversation for this
# protocol. Internal, use GoRpcClient instead.
class _GoRpcConn(object):
  def __init__(self, timeout):
    self.conn = None
    self.timeout = timeout
    self.start_time = None

  def dial(self, uri):
    parts = urlparse.urlparse(uri)
    netloc = parts.netloc.split(':')
    # NOTE(msolomon) since the deadlines are approximate in the code, set
    # timeout to oversample to minimize waiting in the extreme failure mode.
    socket_timeout = self.timeout / 10.0
    self.conn = socket.create_connection((netloc[0], int(netloc[1])),
                                         socket_timeout)
    self.conn.sendall('CONNECT %s HTTP/1.0\n\n' % parts.path)
    while True:
      data = self.conn.recv(1024)
      if not data:
        raise GoRpcError('Unexpected EOF in handshake')
      if '\n\n' in data:
        return

  def close(self):
    if self.conn:
      self.conn.close()
      self.conn = None

  def _check_deadline_exceeded(self):
    if (time.time() - self.start_time) > self.timeout:
      raise socket.timeout('deadline exceeded')
    return False

  def write_request(self, request_data):
    self.start_time = time.time()
    self.conn.sendall(request_data)

  # FIXME(msolomon) This makes a couple of assumptions from bson encoding.
  def read_response(self):
    if self.start_time is None:
      raise GoRpcError('no request pending')

    try:
      buf = []
      buf_write = buf.append
      data, data_len = _read_more(self.conn, buf, buf_write)

      # must read at least enough to get the length
      while data_len < len_struct_size and not self._check_deadline_exceeded():
        data, data_len = _read_more(self.conn, buf, buf_write)

      # header_len is the size of the entire header including the length
      # add on an extra len_struct_size to get enough of the body to read size
      header_len = unpack_length(data)[0]
      while (data_len < (header_len + len_struct_size) and
             not self._check_deadline_exceeded()):
        data, data_len = _read_more(self.conn, buf, buf_write)

      # body_len is the size of the entire body - same as above
      body_len = unpack_length(data, header_len)[0]
      total_len = header_len + body_len
      while data_len < total_len and not self._check_deadline_exceeded():
        data, data_len = _read_more(self.conn, buf, buf_write)

      return data
    finally:
      self.start_time = None

def _read_more(conn, buf, buf_write):
  try:
    data = conn.recv(default_read_buffer_size)
    if not data:
      # We only read when we expect data - if we get nothing this probably
      # indicates that the server hung up. This exception ensure the client
      # tears down properly.
      raise socket.error(errno.EPIPE, 'unexpected EOF in read')
  except socket.timeout:
    # catch the timeout and return empty data for now - this breaks the call
    # and lets the deadline get caught with reasonable precision.
    data = ''

  if buf:
    buf_write(data)
    data = _join(buf)
  else:
    buf_write(data)
  return data, _len(data)


class GoRpcClient(object):
  def __init__(self, uri, timeout):
    self.uri = uri
    self.timeout = timeout
    # FIXME(msolomon) make this random initialized?
    self.seq = 0
    self._conn = None

  @property
  def conn(self):
    if not self._conn:
      self._conn = _GoRpcConn(self.timeout)
      self._conn.dial(self.uri)
    return self._conn

  def close(self):
    if self._conn:
      self._conn.close()
      self._conn = None

  def next_sequence_id(self):
    self.seq += 1
    return self.seq

  # return encoded request data, including header
  def encode_request(self, req):
    raise NotImplementedError

  # fill response with decoded data
  def decode_response(self, response, data):
    raise NotImplementedError

  # Perform an rpc, raising a GoRpcError, on errant situations.
  # Pass in a response object if you don't want a generic one created.
  def call(self, method, request, response=None):
    try:
      h = make_header(method, self.next_sequence_id())
      req = GoRpcRequest(h, request)
      self.conn.write_request(self.encode_request(req))
      data = self.conn.read_response()
      if response is None:
        response = GoRpcResponse()
      self.decode_response(response, data)
    except socket.timeout, e:
      # tear down - can't guarantee a clean conversation
      self.close()
      raise TimeoutError(e, self.timeout, method)
    except socket.error, e:
      # tear down - better chance of recovery by reconnecting
      self.close()
      raise GoRpcError(e, method)

    if response.error:
      raise AppError(response.error, method)

    if response.sequence_id != req.sequence_id:
      # tear down - off-by-one error in the connection somewhere
      self.close()
      raise GoRpcError('request sequence mismatch', response.sequence_id,
                       req.sequence_id, method)
    return response
