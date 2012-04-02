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
