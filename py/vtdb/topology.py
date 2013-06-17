# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# Give a somewhat sane API to the topology, using zkocc as the backend.
#
# There are two main use cases for the topology:
#   1. resolve a "db key" into a set of parameters that can be used to connect
#   2. resolve the full topology of all databases
#

import json
import logging
import random

from zk import zkocc
from vtdb import keyspace

# keeps a global version of the topology
__keyspace_map = {}
def get_keyspace(name):
  return __keyspace_map[name]

def __add_keyspace(ks):
  __keyspace_map[ks.name] = ks

# read all the keyspaces, populates __keyspace_map, can call get_keyspace
# after this step
def read_keyspaces(zkocc_client):
  read_topology(zkocc_client, read_fqdb_keys=False)

# ZK paths look like:
# /zk/<cell>/vt/keyspaces/<keyspace>/shards/<shard>/<db_type>/<instance_id>
# this function returns:
# - a list of all the existing <keyspace>.<shard>.<db_type>
# - optionally, a list of all existing endpoints:
#   <keyspace>.<shard>.<db_type>.<instance_id>
def read_topology(zkocc_client, read_fqdb_keys=True):
  fqdb_keys = []
  db_keys = []
  keyspace_list = zkocc_client.children(keyspace.ZK_KEYSPACE_PATH)['Children']
  for keyspace_name in keyspace_list:
    try:
      ks = keyspace.read_keyspace(zkocc_client, keyspace_name)
      __add_keyspace(ks)
      for shard_name in ks.shard_names:
        for db_type in ks.db_types:
          db_key_parts = [ks.name, shard_name, db_type]
          db_key = '.'.join(db_key_parts)
          db_keys.append(db_key)

          if read_fqdb_keys:
            db_instances = len(get_host_port_by_name(zkocc_client, db_key))
            for db_i in xrange(db_instances):
              fqdb_keys.append('.'.join(db_key_parts + [str(db_i)]))

    except Exception:
      logging.exception('error getting or parsing keyspace data for %s',
                        keyspace_name)
  return db_keys, fqdb_keys

# db_key is <keyspace>.<shard_name>.<db_type>[:<service>]
# returns a list of entries to try, which is an array of tuples
# (host, port, encrypted)
def get_host_port_by_name(zkocc_client, db_key, encrypted=False):
  parts = db_key.split(':')
  if len(parts) == 2:
    service = parts[1]
  else:
    service = '_mysql'
  if service == '_vtocc' and encrypted:
    encrypted_service = '_vts'
  db_key = parts[0]
  zk_path = '/zk/local/vt/ns/' + db_key.replace('.', '/') # no protocol here
  try:
    data = zkocc_client.get(zk_path)['Data']
    data = json.loads(data)
  except zkocc.ZkOccError as e:
    logging.warning('no data in %s: %s', zk_path, e)
    return []
  except Exception as e:
    logging.warning('failed to get or parse zk data %s (%s): %s', zk_path, e,
                    data)
    return []
  host_port_list = []
  encrypted_host_port_list = []
  for entry in data['entries']:
    if service in entry['named_port_map']:
      host_port = (entry['host'], entry['named_port_map'][service],
                   service == '_vts')
      host_port_list.append(host_port)
    if encrypted and encrypted_service in entry['named_port_map']:
      host_port = (entry['host'], entry['named_port_map'][encrypted_service],
                   True)
      encrypted_host_port_list.append(host_port)
  if encrypted and len(encrypted_host_port_list) > 0:
    random.shuffle(encrypted_host_port_list)
    return encrypted_host_port_list
  random.shuffle(host_port_list)
  return host_port_list
