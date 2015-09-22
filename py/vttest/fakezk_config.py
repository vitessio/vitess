# Copyright 2013 Google Inc. All Rights Reserved.

"""Generate a config file for fakezk topology."""

__author__ = 'enisoc@google.com (Anthony Yeh)'

import base64
import codecs
import json


class FakeZkConfig(object):
  """Create fakezk config for use as static topology for vtgate."""

  def __init__(self, mysql_port, cell='test_cell', host='127.0.0.1'):
    self.keyspaces = {}
    self.served_from = {}
    self.host = host
    self.cell = cell
    self.mysql_port = mysql_port

  def add_shard(self, keyspace, shard, vt_port, grpc_port=None):
    """Add a shard to the config."""

    # compute the start and end
    start = ''
    end = ''
    if '-' in shard:
      parts = shard.split('-', 2)
      start = parts[0]
      end = parts[1]

    if keyspace not in self.keyspaces:
      self.keyspaces[keyspace] = []

    self.keyspaces[keyspace].append({
        'shard': shard,
        'vt_port': vt_port,
        'grpc_port': grpc_port,
        'start': start,
        'end': end,
    })

  def add_redirect(self, from_keyspace, to_keyspace):
    """Set a keyspace to be ServedFrom another."""
    self.served_from[from_keyspace] = to_keyspace

  def keyspace_id_as_base64(self, s):
    raw = codecs.decode(s, 'hex')
    return base64.b64encode(raw)

  def as_json(self):
    """Return the config as JSON. This is a proto3 version of SrvKeyspace."""

    result = {}
    tablet_types_str = ['master', 'replica', 'rdonly']
    tablet_types_int = [2, 3, 4]
    sharding_colname = 'keyspace_id'
    sharding_coltype = 1
    for keyspace, shards in self.keyspaces.iteritems():
      shard_references = []
      for shard in shards:
        key_range = {}
        if shard['start']:
          key_range['start'] = self.keyspace_id_as_base64(shard['start'])
        if shard['end']:
          key_range['end'] = self.keyspace_id_as_base64(shard['end'])
        shard_references.append({
            'name': shard['shard'],
            'key_range': key_range,
        })
        for dbtype in tablet_types_str:
          path = '/zk/%s/vt/ns/%s/%s/%s' % (self.cell, keyspace,
                                            shard['shard'], dbtype)
          port_map = {
              'mysql': self.mysql_port,
              'vt': shard['vt_port'],
              }
          if shard['grpc_port']:
            port_map['grpc'] = shard['grpc_port']
          result[path] = {
              'entries': [
                  {
                      'uid': 0,
                      'host': self.host,
                      'port_map': port_map,
                  },
              ],
          }

      path = '/zk/%s/vt/ns/%s' % (self.cell, keyspace)
      partitions = []
      for tablet_type in tablet_types_int:
        partitions.append({
            'served_type': tablet_type,
            'shard_references': shard_references,
        })
      result[path] = {
          'partitions': partitions,
          'sharding_column_name': sharding_colname,
          'sharding_column_type': sharding_coltype,
      }

    for from_keyspace, to_keyspace in self.served_from.iteritems():
      path = '/zk/%s/vt/ns/%s' % (self.cell, from_keyspace)
      served_from = []
      for dbtype in tablet_types_int:
        served_from.append({
            'tablet_type': dbtype,
            'keyspace': to_keyspace,
            })
      result[path] = {
          'served_from': served_from,
      }

    return json.dumps(result)
