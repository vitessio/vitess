import json
import logging
import os
import random
import threading

from net import bsonrpc
from net import gorpc

class ZkOccError(Exception):
  pass

#
# the ZkNode dict returned by these structures has the following members:
#
# Path     string
# Data     string
# Stat     ZkStat
# Children []string
# Cached   bool // the response comes from the zkocc cache
# Stale    bool // the response is stale because we're not connected
#
# ZkStat is a dict:
#
# czxid          long
# mzxid          long
# cTime          time.DateTime
# mTime          time.DateTime
# version        int
# cVersion       int
# aVersion       int
# ephemeralOwner long
# dataLength     int
# numChildren    int
# pzxid          long
#

# A simple, direct connection to a single zkocc server. Doesn't retry.
# You probably want to use ZkOccConnection instead.
class SimpleZkOccConnection(object):

  def __init__(self, addr, timeout, user=None, password=None):
    self.client = bsonrpc.BsonRpcClient(addr, timeout, user, password)

  def dial(self):
    self.client.dial()

  def close(self):
    self.client.close()

  def _call(self, method, **kwargs):
    req = dict((''.join(w.capitalize() for w in k.split('_')), v)
               for k, v in kwargs.items())
    try:
      return self.client.call(method, req).reply
    except gorpc.GoRpcError as e:
      raise ZkOccError('%s %s failed' % (method, req), e)

  # returns a ZkNode, see header
  def get(self, path):
    return self._call('ZkReader.Get', path=path)

  # returns an array of ZkNode, see header
  def getv(self, paths):
    return self._call('ZkReader.GetV', paths=paths)

  # returns a ZkNode, see header
  def children(self, path):
    return self._call('ZkReader.Children', path=path)

  def get_srv_keyspace_names(self, cell):
    return self._call('TopoReader.GetSrvKeyspaceNames', cell=cell)['Entries']

  def get_srv_keyspace(self, cell, keyspace):
    return self._call('TopoReader.GetSrvKeyspace', cell=cell, keyspace=keyspace)

  def get_end_points(self, cell, keyspace, shard, tablet_type):
    return self._call('TopoReader.GetEndPoints', cell=cell, keyspace=keyspace, shard=shard, tablet_type=tablet_type)


# A meta-connection that can connect to multiple alternate servers, and will
# retry a couple times. Calling dial before get/getv/children is optional,
# and will only do anything at all if authentication is enabled.
class ZkOccConnection(object):
  max_attempts = 2
  max_dial_attempts = 10

  # addrs is a comma separated list of server:ip pairs.
  def __init__(self, addrs, local_cell, timeout, user=None, password=None):
    self.timeout = timeout
    self.addrs = addrs.split(',')
    self.local_cell = local_cell

    if bool(user) != bool(password):
      raise ValueError("You must provide either both or none of user and password.")
    self.user = user
    self.password = password

    self.simple_conn = None
    self.lock = threading.Lock()

  def _resolve_path(self, zk_path):
    # Maps a 'meta-path' to a cell specific path.
    # '/zk/local/blah' -> '/zk/vb/blah'
    parts = zk_path.split('/')

    if len(parts) < 3:
      return zk_path

    if parts[2] != 'local':
      return zk_path

    parts[2] = self.local_cell
    return '/'.join(parts)

  # addrs is a comma separated list of server:ip pairs.
  def refresh_addrs(self, addrs):
    self.addrs = addrs.split(',')
    if self.simple_conn.client.addr not in self.addrs:
      self.close()

  def dial(self):
    if self.simple_conn:
      self.simple_conn.close()

    addrs = random.sample(self.addrs, min(self.max_dial_attempts, len(self.addrs)))
    for a in addrs:
      self.simple_conn = SimpleZkOccConnection(a, self.timeout, self.user, self.password)
      try:
        self.simple_conn.dial()
        return
      except:
        pass

    self.simple_conn = None
    raise ZkOccError("Cannot dial to any server, tried: %s" % addrs)

  def close(self):
    if self.simple_conn:
      self.simple_conn.close()
      self.simple_conn = None

  def _call(self, client_method, *args, **kwargs):
    with self.lock:
      if not self.simple_conn:
        self.dial()

      attempt = 0
      while True:
        try:
          return getattr(self.simple_conn, client_method)(*args, **kwargs)
        except Exception as e:
          attempt += 1
          logging.warning('zkocc: %s command failed %u times: %s', client_method, attempt, e)
          if attempt >= self.max_attempts:
            raise ZkOccError('zkocc %s command failed %u times: %s' % (client_method, attempt, e))

          # try the next server if there is one, or retry our only server
          self.dial()

  # New API.

  def get_srv_keyspace_names(self, cell):
    if cell == 'local':
      cell = self.local_cell
    return self._call('get_srv_keyspace_names', cell=cell)

  def get_srv_keyspace(self, cell, keyspace):
    if cell == 'local':
      cell = self.local_cell
    return self._call('get_srv_keyspace', cell=cell, keyspace=keyspace)

  def get_end_points(self, cell, keyspace, shard, tablet_type):
    if cell == 'local':
      cell = self.local_cell
    return self._call('get_end_points', cell=cell, keyspace=keyspace,
                      shard=shard, tablet_type=tablet_type)

  # Old, deprecated API.

  # returns a ZkNode, see header
  def get(self, path):
    return self._call('get', self._resolve_path(path))

  # returns an array of ZkNode, see header
  def getv(self, paths):
    return self._call('getv', [self._resolve_path(p) for p in paths])

  # returns a ZkNode, see header
  def children(self, path):
    return self._call('children', self._resolve_path(path))

# use this class for faking out a zkocc client. The startup config values
# can be loaded from a json file. After that, they can be mass-altered
# to replace default values with test-specific values, for instance.
class FakeZkOccConnection(object):
  def __init__(self, local_cell):
    self.data = {}
    self.local_cell = local_cell

  @classmethod
  def from_data_path(cls, local_cell, data_path):
    # Returns client with data at given data_path loaded.
    client = cls(local_cell)
    with open(data_path) as f:
      data = f.read()
    for key, value in json.loads(data).iteritems():
      client.data[key] = json.dumps(value)
    return client

  def replace_zk_data(self, before, after):
    # Does a string substitution on all zk data.
    # This is for testing purpose only.
    for key, data in self.data.iteritems():
      self.data[key] = data.replace(before, after)

  def _resolve_path(self, zk_path):
    # Maps a 'meta-path' to a cell specific path.
    # '/zk/local/blah' -> '/zk/vb/blah'
    parts = zk_path.split('/')

    if len(parts) < 3:
      return zk_path

    if parts[2] != 'local':
      return zk_path

    parts[2] = self.local_cell
    return '/'.join(parts)

  def dial(self):
    pass

  def close(self):
    pass

  # Old, deprecated API.

  def get(self, path):
    path = self._resolve_path(path)
    if not path in self.data:
      raise ZkOccError("FakeZkOccConnection: not found: " + path)
    return {
        'Data':self.data[path],
        'Children':[]
        }

  def getv(self, paths):
    raise ZkOccError("FakeZkOccConnection: not found: " + " ".join(paths))

  def children(self, path):
    path = self._resolve_path(path)
    children = [os.path.basename(node) for node in self.data
                if os.path.dirname(node) == path]
    if len(children) == 0:
      raise ZkOccError("FakeZkOccConnection: not found: " + path)
    return {
        'Data':'',
        'Children':children
        }

  # New API. For this fake object, it is based on the old API.

  def get_srv_keyspace_names(self, cell):
    if cell == 'local':
      cell = self.local_cell
    return self.children('/zk/' + cell + '/vt/ns')['Children']

  def get_srv_keyspace(self, cell, keyspace):
    keyspace_path = '/zk/' + cell + '/vt/ns/' + keyspace
    try:
      data = self.get(keyspace_path)['Data']
      if not data:
        raise ZkOccError("FakeZkOccConnection: empty keyspace: " + keyspace)
      result = json.loads(data)
      # for convenience, we store the KeyRange as hex, but we need to
      # decode it here, as BSON RPC sends it as binary.
      if 'Shards' in result:
        for shard in result['Shards']:
          shard['KeyRange']['Start'] = shard['KeyRange']['Start'].decode('hex')
          shard['KeyRange']['End'] = shard['KeyRange']['End'].decode('hex')
      return result
    except Exception as e:
      raise ZkOccError('FakeZkOccConnection: invalid keyspace', keyspace, e)

  def get_end_points(self, cell, keyspace, shard, tablet_type):
    zk_path = os.path.join('/zk', cell, 'vt', 'ns', keyspace, shard,
                           tablet_type)
    try:
      data = self.get(zk_path)['Data']
      if not data:
        raise ZkOccError("FakeZkOccConnection: empty end point: " + zk_path)
      return json.loads(data)
    except Exception as e:
      raise ZkOccError('FakeZkOccConnection: invalid end point', zk_path, e)
