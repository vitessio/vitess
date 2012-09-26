# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import hmac

from net import bsonrpc
from net import gorpc

class ZkOccError(Exception):
  pass

# A simple, direct connection to the zkocc server.
class ZkOccConnection(object):

  def __init__(self, addr, timeout, user=None, password=None):
    self.addr = addr
    self.timeout = timeout

    if bool(user) != bool(password):
      raise ValueError("You must provide either both or none of user and password.")
    self.user = user
    self.password = password
    self.use_auth = bool(user)

    self.client = bsonrpc.BsonRpcClient(self.uri, self.timeout)

  def dial(self):
    if self.client:
      self.client.close()
    if self.use_auth:
      self.authenticate()

  def authenticate(self):
    challenge = self.client.call('AuthenticatorCRAMMD5.GetNewChallenge', "").reply['Challenge']
    # CRAM-MD5 authentication.
    proof = self.user + " " + hmac.HMAC(self.password, challenge).hexdigest()
    self.client.call('AuthenticatorCRAMMD5.Authenticate', {"Proof": proof})

  @property
  def uri(self):
    if self.use_auth:
      return 'http://%s/_bson_rpc_/auth' % self.addr
    return 'http://%s/_bson_rpc_' % self.addr

  def close(self):
    self.client.close()

  __del__ = close

  def get(self, path):
    req = {'Path':path}
    try:
      return self.client.call('ZkReader.Get', req).reply
    except gorpc.GoRpcError as e:
      raise ZkOccError('get failed', e)

  def getv(self, paths):
    req = {'Paths':paths}
    try:
      return self.client.call('ZkReader.GetV', req).reply
    except gorpc.GoRpcError as e:
      raise ZkOccError('getv failed', e)

  def children(self, path):
    req = {'Path':path}
    try:
      return self.client.call('ZkReader.Children', req).reply
    except gorpc.GoRpcError as e:
      raise ZkOccError('children failed', e)
