# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# Handle transport and serialization callbacks for Go-style RPC servers.
#
# This is pretty simple. The client initiates an HTTP CONNECT and then
# hijacks the socket. The client is synchronous, but implements deadlines.

import errno
import select
import ssl
import socket
import time
import urlparse

_lastStreamResponseError = 'EOS'

class GoRpcError(Exception):
  pass

class TimeoutError(GoRpcError):
  pass


# The programmer has misused an API, but the underlying
# connection is still salvagable.
class ProgrammingError(GoRpcError):
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
    # FIXME(msolomon) reimplement using deadlines
    self.socket_timeout = timeout / 10.0
    self.buf = []

  def dial(self, uri, keyfile=None, certfile=None):
    parts = urlparse.urlparse(uri)
    conhost, conport = parts.netloc.split(':')
    try:
      conip = socket.gethostbyname(conhost)
    except NameError:
      conip = socket.getaddrinfo(conhost, None)[0][4][0]
    self.conn = socket.create_connection((conip, int(conport)), self.socket_timeout)
    if parts.scheme == 'https':
      self.conn = ssl.wrap_socket(self.conn, keyfile=keyfile, certfile=certfile)
    self.conn.sendall('CONNECT %s HTTP/1.0\n\n' % parts.path)
    while True:
      data = self.conn.recv(1024)
      if not data:
        raise GoRpcError('Unexpected EOF in handshake to %s:%s %s' % (str(conip), str(conport), parts.path))
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
        # indicates that the server hung up. This exception ensures the client
        # tears down properly.
        raise socket.error(errno.EPIPE, 'unexpected EOF in read')
    except socket.timeout:
      # catch the timeout and return empty data for now - this breaks the call
      # and lets the deadline get caught with reasonable precision.
      return None
    except ssl.SSLError as e:
      # another possible timeout condition with SSL wrapper
      if 'timed out' in str(e):
        return None
      raise
    except socket.error as e:
      if e.args[0] == errno.EINTR:
        # We were interrupted, let the caller retry.
        return None
      raise

    return data

  def is_closed(self):
    if self.conn is None:
      return True

    # make sure the socket hasn't gone away
    fileno = self.conn.fileno()
    poll = select.poll()
    poll.register(fileno)
    ready = poll.poll(0)
    if ready:
      _, event = ready[0]
      if event & select.POLLIN:
        return True
    return False


class GoRpcClient(object):
  def __init__(self, uri, timeout, certfile=None, keyfile=None):
    self.uri = uri
    self.timeout = timeout
    self.start_time = None
    # FIXME(msolomon) make this random initialized?
    self.seq = 0
    self.conn = None
    self.data = None
    self.certfile = certfile
    self.keyfile = keyfile

  def dial(self):
    if self.conn:
      self.close()
    conn = _GoRpcConn(self.timeout)
    try:
      conn.dial(self.uri, self.certfile, self.keyfile)
    except socket.timeout as e:
      raise TimeoutError(e, self.timeout, 'dial', self.uri)
    except ssl.SSLError as e:
      # another possible timeout condition with SSL wrapper
      if 'timed out' in str(e):
        raise TimeoutError(e, self.timeout, 'dial', self.uri)
      raise GoRpcError(e)
    except socket.error as e:
      raise GoRpcError(e)
    self.conn = conn

  def close(self):
    if self.conn:
      self.conn.close()
      self.conn = None
    self.start_time = None

  def is_closed(self):
    if self.conn:
      if self.conn.is_closed():
        return True
      else:
        return False
    return True

  __del__ = close

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

  def _check_deadline_exceeded(self, timeout):
    if (time.time() - self.start_time) > timeout:
      raise socket.timeout('deadline exceeded')

  # logic to read the next response off the wire
  def _read_response(self, response, timeout):
    if self.start_time is None:
      raise ProgrammingError('no request pending')
    if not self.conn:
      raise GoRpcError('closed client')

    # get some data if we don't have any so we have somewhere to start
    if self.data is None:
      while True:
        self.data = self.conn.read_some()
        if self.data:
          break
        self._check_deadline_exceeded(timeout)

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
          self._check_deadline_exceeded(timeout)
        self.data += more_data

  # Perform an rpc, raising a GoRpcError, on errant situations.
  # Pass in a response object if you don't want a generic one created.
  def call(self, method, request, response=None):
    if not self.conn:
      raise GoRpcError('closed client', method)
    try:
      h = make_header(method, self.next_sequence_id())
      req = GoRpcRequest(h, request)
      self.start_time = time.time()
      self.conn.write_request(self.encode_request(req))
      if response is None:
        response = GoRpcResponse()
      self._read_response(response, self.timeout)
      self.start_time = None
    except socket.timeout as e:
      # tear down - can't guarantee a clean conversation
      self.close()
      raise TimeoutError(e, self.timeout, method)
    except socket.error as e:
      # tear down - better chance of recovery by reconnecting
      self.close()
      raise GoRpcError(e, method)
    except ssl.SSLError as e:
      # tear down - better chance of recovery by reconnecting
      self.close()
      if 'timed out' in str(e):
        raise TimeoutError(e, self.timeout, method)
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
    if not self.conn:
      raise GoRpcError('closed client', method)
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
    except ssl.SSLError as e:
      # tear down - better chance of recovery by reconnecting
      self.close()
      if 'timed out' in str(e):
        raise TimeoutError(e, self.timeout, method)
      raise GoRpcError(e, method)

  # Returns the next value, or None if we're done.
  # Note the timeout is longer as we don't mind for streaming queries
  # since they get their own bigger connection pool on the vttablet side
  # FIXME(alainjobart) The timeout needs to be passed in, not inferred,
  # otherwise it's going to be hard to debug... Anyway, the value
  # for the timeout will most likely be 300s here, as timeout is usually 30s.
  def stream_next(self):
    try:
      response = GoRpcResponse()
      self._read_response(response, self.timeout * 10)
    except socket.timeout as e:
      # tear down - can't guarantee a clean conversation
      self.close()
      raise TimeoutError(e, self.timeout)
    except socket.error as e:
      # tear down - better chance of recovery by reconnecting
      self.close()
      raise GoRpcError(e)
    except ssl.SSLError as e:
      # tear down - better chance of recovery by reconnecting
      self.close()
      if 'timed out' in str(e):
        raise TimeoutError(e, self.timeout)
      raise GoRpcError(e)

    if response.sequence_id != self.seq:
      # tear down - off-by-one error in the connection somewhere
      self.close()
      raise GoRpcError('request sequence mismatch', response.sequence_id,
                       self.req)

    if response.error:
      self.start_time = None
      if response.error == _lastStreamResponseError:
        return None
      else:
        raise AppError(response.error)
    else:
      self.start_time = time.time()

    return response
