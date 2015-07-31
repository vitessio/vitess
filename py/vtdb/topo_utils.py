# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import random

from zk import zkocc
from vtdb import topology
from vtdb import vtdb_logger


class VTConnParams(object):
  keyspace = None
  shard = None
  db_type = None
  addr = None
  timeout = 0
  user = None
  password = None

  def __init__(self, keyspace_name, shard, db_type, addr, timeout,
               user, password):
    self.keyspace = keyspace_name
    self.shard = shard
    self.tablet_type = db_type
    self.addr = addr
    self.timeout = timeout
    self.user = user
    self.password = password


def get_db_params_for_tablet_conn(topo_client, keyspace_name, shard, db_type, timeout, user, password):
  db_params_list = []
  db_key = "%s.%s.%s:vt" % (keyspace_name, shard, db_type)
  # This will read the cached keyspace.
  keyspace_object = topology.get_keyspace(keyspace_name)

  # Handle vertical split by checking 'ServedFrom' field.
  new_keyspace = None
  served_from = keyspace_object.served_from
  if served_from is not None:
    new_keyspace = served_from.get(db_type, None)
    if new_keyspace is not None:
      keyspace_name = new_keyspace

  try:
    end_points_data = topo_client.get_end_points('local', keyspace_name, shard, db_type)
  except zkocc.ZkOccError as e:
    vtdb_logger.get_logger().topo_zkocc_error('do data', db_key, e)
    return []
  except Exception as e:
    vtdb_logger.get_logger().topo_exception('failed to get or parse topo data', db_key, e)
    return []

  host_port_list = []
  if 'Entries' not in end_points_data:
    vtdb_logger.get_logger().topo_exception('topo server returned: ' + str(end_points_data), db_key, e)
    raise Exception('zkocc returned: %s' % str(end_points_data))
  for entry in end_points_data['Entries']:
    if 'vt' in entry['PortMap']:
      host_port = (entry['Host'], entry['PortMap']['vt'])
      host_port_list.append(host_port)
  random.shuffle(host_port_list)

  for host, port in host_port_list:
    vt_params = VTConnParams(keyspace_name, shard, db_type, "%s:%s" % (host, port), timeout, user, password).__dict__
    db_params_list.append(vt_params)
  return db_params_list
