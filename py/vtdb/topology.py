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
import time

from vtdb import dbexceptions
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import keyspace
from vtdb import vtdb_logger
from zk import zkocc


# keeps a global version of the topology
# This is a map of keyspace_name: (keyspace object, time when keyspace was last fetched)
# eg - {'keyspace_name': (keyspace_object, time_of_last_fetch)}
# keyspace object is defined at py/vtdb/keyspace.py:Keyspace
__keyspace_map = {}


# Throttle to clear the keyspace cache and re-read it.
__keyspace_fetch_throttle = 5


def set_keyspace_fetch_throttle(throttle):
  global __keyspace_fetch_throttle
  __keyspace_fetch_throttle = throttle


# This returns the keyspace object for the keyspace name
# from the cached topology map or None if not found.
def get_keyspace(name):
  try:
    return __keyspace_map[name][0]
  except KeyError:
    return None


# This returns the time of last fetch for the keyspace name
# from the cached topology map or None if not found.
def get_time_last_fetch(name):
  try:
    return __keyspace_map[name][1]
  except KeyError:
    return None


# This adds the keyspace object to the cached topology map.
def __set_keyspace(ks):
  __keyspace_map[ks.name] = (ks, time.time())


# This function refreshes the keyspace in the cached topology
# map throttled by __keyspace_fetch_throttle secs. If the topo
# server is unavailable, it retains the old keyspace object.
def refresh_keyspace(zkocc_client, name):
  global __keyspace_fetch_throttle

  time_last_fetch = get_time_last_fetch(name)
  if time_last_fetch is None:
    return

  if (time_last_fetch + __keyspace_fetch_throttle) > time.time():
    return

  start_time = time.time()
  ks = keyspace.read_keyspace(zkocc_client, name)
  topo_rtt = time.time() - start_time
  if ks is not None:
    __set_keyspace(ks)

  vtdb_logger.get_logger().topo_keyspace_fetch(name, topo_rtt)


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
    vtdb_logger.get_logger().topo_empty_keyspace_list()
    raise Exception('zkocc returned empty keyspace list')
  for keyspace_name in keyspace_list:
    try:
      ks = keyspace.read_keyspace(zkocc_client, keyspace_name)
      __set_keyspace(ks)
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
      vtdb_logger.get_logger().topo_bad_keyspace_data(keyspace_name)
  return db_keys, fqdb_keys


# db_key is <keyspace>.<shard_name>.<db_type>[:<service>]
# returns a list of entries to try, which is an array of tuples
# (host, port, encrypted)
def get_host_port_by_name(topo_client, db_key, encrypted=False):
  parts = db_key.split(':')
  if len(parts) == 2:
    service = parts[1]
  else:
    service = '_mysql'

  host_port_list = []
  encrypted_host_port_list = []

  if service == '_vtocc' and encrypted:
    encrypted_service = '_vts'
  db_key = parts[0]
  ks, shard, tablet_type = db_key.split('.')
  try:
    data = topo_client.get_end_points('local', ks, shard, tablet_type)
  except zkocc.ZkOccError as e:
    vtdb_logger.get_logger().topo_zkocc_error('do data', db_key, e)
    return []
  except Exception as e:
    vtdb_logger.get_logger().topo_exception('failed to get or parse topo data', db_key, e)
    return []
  if 'Entries' not in data:
    vtdb_logger.get_logger().topo_exception('topo server returned: ' + str(data), db_key, e)
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


def is_sharded_keyspace(keyspace_name, db_type):
  ks = get_keyspace(keyspace_name)
  shard_count = ks.get_shard_count(db_type)
  return shard_count > 1

def get_keyrange_from_shard_name(keyspace, shard_name):
  kr = None
  # db_type is immaterial here.
  if not is_sharded_keyspace(keyspace, 'replica'):
    if shard_name == keyrange_constants.SHARD_ZERO:
      kr = keyrange_constants.NON_PARTIAL_KEYRANGE
    else:
      raise dbexceptions.DatabaseError('Invalid shard_name %s for keyspace %s', shard_name, keyspace)
  else:
    kr_parts = shard_name.split('-')
    if len(kr_parts) != 2:
      raise dbexceptions.DatabaseError('Invalid shard_name %s for keyspace %s', shard_name, keyspace)
    kr = keyrange.KeyRange((kr_parts[0].decode('hex'), kr_parts[1].decode('hex')))
  return kr
