# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# Go-style RPC client using BSON as the codec.

import bson
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


class BsonRpcClient(gorpc.GoRpcClient):
  def encode_request(self, req):
    try:
      if not isinstance(req.body, dict):
        # hack to handle simple values
        body = {WRAPPED_FIELD: req.body}
      else:
        body = req.body
      return bson.dumps(req.header) + bson.dumps(body)
    except Exception, e:
      raise gorpc.GoRpcError('encode error', e)

  # fill response with decoded data
  def decode_response(self, response, data):
    try:
      offset, response.header = decode_document(data, 0)
      offset, response.reply = decode_document(data, offset)
      # unpack primitive values
      # FIXME(msolomon) remove this hack
      response.reply = response.reply.get(WRAPPED_FIELD, response.reply)
    except Exception, e:
      raise gorpc.GoRpcError('decode error', e)
