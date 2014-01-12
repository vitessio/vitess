# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# Give a somewhat sane API to the topology, using zkocc as the backend.
#
# There are two main use cases for the topology:
#   1. resolve a "db key" into a set of parameters that can be used to connect
#   2. resolve the full topology of all databases
#

import logging
import random

from vtdb import keyspace
from zk import zkocc

# keeps a global version of the topology
__keyspace_map = {}


def read_and_get_keyspace(zkocc_client, name):
  ks = None
  try:
    ks = __keyspace_map[name]
  except KeyError:
    ks = keyspace.read_keyspace(zkocc_client, name)
    if ks is not None:
      __add_keyspace(ks)
    else:
      raise
  return ks


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
  keyspace_list = zkocc_client.get_srv_keyspace_names('local')
  # validate step
  if len(keyspace_list) == 0:
    logging.exception('zkocc returned empty keyspace list')
    raise Exception('zkocc returned empty keyspace list')
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
def get_host_port_by_name(zkocc_client, db_key, encrypted=False, vtgate_protocol='v0', vtgate_addrs=None):
  parts = db_key.split(':')
  if len(parts) == 2:
    service = parts[1]
  else:
    service = '_mysql'

  host_port_list = []
  encrypted_host_port_list = []
  # use given vtgate addrs if vtgate is enabled and requested as service
  if vtgate_addrs is None:
    vtgate_addrs = []
  if vtgate_protocol != 'v0' and service != '_mysql':
    for addr in vtgate_addrs:
      host_port = addr.split(':')
      host_port_list.append((host_port[0], long(host_port[1]), service == '_vts'))
    random.shuffle(host_port_list)
    return host_port_list

  if service == '_vtocc' and encrypted:
    encrypted_service = '_vts'
  db_key = parts[0]
  ks, shard, tablet_type = db_key.split('.')
  try:
    data = zkocc_client.get_end_points('local', ks, shard, tablet_type)
  except zkocc.ZkOccError as e:
    logging.warning('no data for %s: %s', db_key, e)
    return []
  except Exception as e:
    logging.warning('failed to get or parse topo data %s (%s): %s', db_key, e,
                    data)
    return []
  if 'Entries' not in data:
    raise Exception('zkocc returned: %s' % str(data))
  for entry in data['Entries']:
    if service in entry['NamedPortMap']:
      host_port = (entry['Host'], entry['NamedPortMap'][service],
                   service == '_vts')
      host_port_list.append(host_port)
    if encrypted and encrypted_service in entry['NamedPortMap']:
      host_port = (entry['Host'], entry['NamedPortMap'][encrypted_service],
                   True)
      encrypted_host_port_list.append(host_port)
  if encrypted and len(encrypted_host_port_list) > 0:
    random.shuffle(encrypted_host_port_list)
    return encrypted_host_port_list
  random.shuffle(host_port_list)
  return host_port_list
