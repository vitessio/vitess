# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import logging
import random

from zk import zkocc


class VTConnParams(object):
  keyspace = None
  shard = None
  db_type = None
  addr = None
  timeout = 0
  encrypted = False
  user = None
  password = None

  def __init__(self, keyspace, shard, db_type, addr, timeout, encrypted, user, password):
    self.keyspace = keyspace
    self.shard = shard
    self.tablet_type = db_type
    self.addr = addr
    self.timeout = timeout
    self.encrypted = encrypted
    self.user = user
    self.password = password


def get_db_params_for_vtgate_conn(vtgate_addrs, keyspace, shard, db_type, timeout, encrypted, user, password):
  db_params_list = []
  random.shuffle(vtgate_addrs)
  for addr in vtgate_addrs:
    vt_params = VTConnParams(keyspace, shard, db_type, addr, timeout, encrypted, user, password).__dict__
    db_params_list.append(vt_params)
  return db_params_list


def get_db_params_for_tablet_conn(topo_client, keyspace, shard, db_type, timeout, encrypted, user, password):
  db_params_list = []
  encrypted_service = '_vts'
  if encrypted:
    service = encrypted_service
  else:
    service = '_vtocc'
  db_key = "%s.%s.%s:%s" % (keyspace, shard, db_type, service)
  keyspace_data = topo_client.get_srv_keyspace('local', keyspace) 

  # Handle vertical split by checking 'ServedFrom' field.
  new_keyspace = None
  served_from = keyspace_data.get('ServedFrom', None)
  if served_from is not None:
    new_keyspace = served_from.get(db_type, None)
    if new_keyspace is not None:
      keyspace = new_keyspace

  try:
    end_points_data = topo_client.get_end_points('local', keyspace, shard, db_type)
  except zkocc.ZkOccError as e:
    logging.warning('no data for %s: %s', db_key, e)
    return []
  except Exception as e:
    logging.warning('failed to get or parse topo data %s (%s): %s', db_key, e,
                    end_points_data)
    return []

  end_points_list = []
  host_port_list = []
  encrypted_host_port_list = []
  if 'Entries' not in end_points_data:
    raise Exception('zkocc returned: %s' % str(end_points_data))
  for entry in end_points_data['Entries']:
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
    end_points_list = encrypted_host_port_list 
  else:
    random.shuffle(host_port_list)
    end_points_list = host_port_list 


  for host, port, encrypted in end_points_list:
    vt_params = VTConnParams(keyspace, shard, db_type, "%s:%s" % (host, port), timeout, encrypted, user, password).__dict__
    db_params_list.append(vt_params)
  return db_params_list
