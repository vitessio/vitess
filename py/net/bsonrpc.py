# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# Go-style RPC client using BSON as the codec.

import bson
import struct
try:
  # use optimized cbson which has slightly different API
  import cbson
  decode_document = cbson.decode_next
except ImportError:
  from bson import codec
  decode_document = codec.decode_document

from net import gorpc

# Field name used for wrapping simple values as bson documents
# FIXME(msolomon) abandon this - too nasty when protocol requires upgrade
WRAPPED_FIELD = '_Val_'

len_struct = struct.Struct('<i')
unpack_length = len_struct.unpack_from
len_struct_size = len_struct.size

class BsonRpcClient(gorpc.GoRpcClient):
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
      offset, response.header = decode_document(data, 0)
      offset, response.reply = decode_document(data, offset)
      # unpack primitive values
      # FIXME(msolomon) remove this hack
      response.reply = response.reply.get(WRAPPED_FIELD, response.reply)

      # the pure-python bson library returns the offset in the buffer
      # the cbson library returns -1 if everything was read
      # so we cannot use the 'offset' variable. Instead use
      # header_len + body_len for the complete length read

      return header_len + body_len, None
    except Exception as e:
      raise gorpc.GoRpcError('decode error', e)
