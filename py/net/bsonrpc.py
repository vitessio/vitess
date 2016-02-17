# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# Go-style RPC client using BSON as the codec.

import hmac
import struct

import bson

from net import gorpc

# Field name used for wrapping simple values as bson documents
# FIXME(msolomon) abandon this - too nasty when protocol requires upgrade
WRAPPED_FIELD = '_Val_'

len_struct = struct.Struct('<i')
unpack_length = len_struct.unpack_from
len_struct_size = len_struct.size


class BsonRpcClient(gorpc.GoRpcClient):

  def __init__(self, addr, timeout, user=None, password=None,
               keyfile=None, certfile=None):
    if bool(user) != bool(password):
      raise ValueError(
          'You must provide either both or none of user and password.')
    self.addr = addr
    self.user = user
    self.password = password
    if self.user:
      uri = 'http://%s/_bson_rpc_/auth' % self.addr
    else:
      uri = 'http://%s/_bson_rpc_' % self.addr
    gorpc.GoRpcClient.__init__(
        self, uri, timeout, keyfile=keyfile, certfile=certfile)

  def dial(self):
    gorpc.GoRpcClient.dial(self)
    if self.user:
      try:
        self.authenticate()
      except gorpc.GoRpcError:
        self.close()
        raise

  def authenticate(self):
    challenge = self.call(
        'AuthenticatorCRAMMD5.GetNewChallenge', '').reply['Challenge']
    # CRAM-MD5 authentication.
    proof = self.user + ' ' + hmac.HMAC(self.password, challenge).hexdigest()
    self.call('AuthenticatorCRAMMD5.Authenticate', {'Proof': proof})

  def encode_request(self, req):
    try:
      if not isinstance(req.body, dict):
        # hack to handle simple values
        body = {WRAPPED_FIELD: req.body}
      else:
        body = req.body
      return bson.dumps(req.header) + bson.dumps(body)
    except Exception as e:
      raise gorpc.GoRpcError('encode error', e)

  # fill response with decoded data, and returns a tuple
  # (bytes to consume if a response was read,
  #  how many bytes are still to read if no response was read and we know)
  def decode_response(self, response, data):
    data_len = len(data)

    # decode the header length if we have enough
    if data_len < len_struct_size:
      return None, None
    header_len = unpack_length(data)[0]
    if data_len < header_len + len_struct_size:
      return None, None

    # decode the payload length and see if we have enough
    body_len = unpack_length(data, header_len)[0]
    if data_len < header_len + body_len:
      return None, header_len + body_len - data_len

    # we have enough data, decode it all
    try:
      offset, response.header = bson.codec.decode_document(data, 0)
      offset, response.reply = bson.codec.decode_document(data, offset)
      # unpack primitive values
      # FIXME(msolomon) remove this hack
      response.reply = response.reply.get(WRAPPED_FIELD, response.reply)

      # Return header_len + body_len instead of the offsets returned
      # by the library, should be the same.
      return header_len + body_len, None
    except Exception as e:
      raise gorpc.GoRpcError('decode error', e)
