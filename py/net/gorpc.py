# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# Handle transport and serialization callbacks for Go-style RPC servers.
#
# This is pretty simple. The client initiates an HTTP CONNECT and then
# hijacks the socket. The client is synchronous, but implements deadlines.

import errno
import socket
import time
import urlparse

_lastStreamResponseError = "_EndOfStream_"

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

default_read_buffer_size = 8192

# A single socket wrapper to handle request/response conversation for this
# protocol. Internal, use GoRpcClient instead.
class _GoRpcConn(object):
  def __init__(self, timeout):
    self.conn = None
    # NOTE(msolomon) since the deadlines are approximate in the code, set
    # timeout to oversample to minimize waiting in the extreme failure mode.
    self.socket_timeout = timeout / 10.0
    self.buf = []

  def dial(self, uri):
    parts = urlparse.urlparse(uri)
    netloc = parts.netloc.split(':')
    self.conn = socket.create_connection((netloc[0], int(netloc[1])),
                                         self.socket_timeout)
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

  def write_request(self, request_data):
    self.conn.sendall(request_data)

  # tries to read some bytes, returns None if it can't because of a timeout
  def read_some(self, size=None):
    if size is None:
      size = default_read_buffer_size
    try:
      data = self.conn.recv(size)
      if not data:
        # We only read when we expect data - if we get nothing this probably
        # indicates that the server hung up. This exception ensure the client
        # tears down properly.
        raise socket.error(errno.EPIPE, 'unexpected EOF in read')
    except socket.timeout:
      # catch the timeout and return empty data for now - this breaks the call
      # and lets the deadline get caught with reasonable precision.
      return None

    return data

class GoRpcClient(object):
  def __init__(self, uri, timeout):
    self.uri = uri
    self.timeout = timeout
    self.start_time = None
    # FIXME(msolomon) make this random initialized?
    self.seq = 0
    self._conn = None
    self.data = None

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
    self.start_time = None

  def next_sequence_id(self):
    self.seq += 1
    return self.seq

  # return encoded request data, including header
  def encode_request(self, req):
    raise NotImplementedError

  # fill response with decoded data, and returns a tuple
  # (bytes to consume if a response was read,
  #  how many bytes are still to read if no response was read and we know)
  def decode_response(self, response, data):
    raise NotImplementedError

  def _check_deadline_exceeded(self):
    if (time.time() - self.start_time) > self.timeout:
      raise socket.timeout('deadline exceeded')
    return False

  # logic to read the next response off the wire
  def read_response(self, response):
    if self.start_time is None:
      raise GoRpcError('no request pending')

    # get some data if we don't have any so we have somewhere to start
    if self.data is None:
      while True:
        self.data = self.conn.read_some()
        if self.data:
          break
        self._check_deadline_exceeded()

    # now try to decode, and read more if we need to
    while True:
      consumed, extra_needed = self.decode_response(response, self.data)
      if consumed:
        data_len = len(self.data)
        if data_len > consumed:
          # we have extra data, keep it
          self.data = self.data[consumed:]
        else:
          # no extra data, nothing to keep
          self.data = None
        return
      else:
        # we don't have enough data, read more, and check the timeout
        # every time
        while True:
          more_data = self.conn.read_some(extra_needed)
          if more_data:
            break
          self._check_deadline_exceeded()
        self.data += more_data

  # Perform an rpc, raising a GoRpcError, on errant situations.
  # Pass in a response object if you don't want a generic one created.
  def call(self, method, request, response=None):
    try:
      h = make_header(method, self.next_sequence_id())
      req = GoRpcRequest(h, request)
      self.start_time = time.time()
      self.conn.write_request(self.encode_request(req))
      if response is None:
        response = GoRpcResponse()
      self.read_response(response)
      self.start_time = None
    except socket.timeout as e:
      # tear down - can't guarantee a clean conversation
      self.close()
      raise TimeoutError(e, self.timeout, method)
    except socket.error as e:
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

  # Perform a streaming rpc call
  # This method doesn't fetch any result, use stream_next to get them
  def stream_call(self, method, request):
    try:
      h = make_header(method, self.next_sequence_id())
      req = GoRpcRequest(h, request)
      self.start_time = time.time()
      self.conn.write_request(self.encode_request(req))
    except socket.timeout as e:
      # tear down - can't guarantee a clean conversation
      self.close()
      raise TimeoutError(e, self.timeout, method)
    except socket.error as e:
      # tear down - better chance of recovery by reconnecting
      self.close()
      raise GoRpcError(e, method)

  # Returns the next value, or None if we're done.
  def stream_next(self):
    try:
      response = GoRpcResponse()
      self.read_response(response)
    except socket.timeout as e:
      # tear down - can't guarantee a clean conversation
      self.close()
      raise TimeoutError(e, self.timeout)
    except socket.error as e:
      # tear down - better chance of recovery by reconnecting
      self.close()
      raise GoRpcError(e)

    if response.sequence_id != self.seq:
      # tear down - off-by-one error in the connection somewhere
      self.close()
      raise GoRpcError('request sequence mismatch', response.sequence_id,
                       self.req)

    if response.error:
      if response.error == _lastStreamResponseError:
        return None
      else:
        raise AppError(response.error)
      self.start_time = None
    else:
      self.start_time = time.time()

    return response
