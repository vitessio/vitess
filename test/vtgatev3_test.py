#!/usr/bin/env python
# coding: utf-8

import hmac
import json
import logging
import os
import struct
import threading
import time
import traceback
import unittest
import urllib

import environment
import tablet
import utils

from vtdb import vtgatev3

conn_class = vtgatev3

shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()

shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()

vtgate_server = None
vtgate_port = None

KEYSPACE_NAME = 'test_keyspace'

create_vt_insert_test = '''create table vt_insert_test (
id bigint,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

schema = '''{
  "Keyspaces": {
    "test_keyspace": {
      "Sharded": true,
      "Vindexes": {
        "id_index": {
          "Type": "hash",
          "Owner": ""
        }
      },
      "Tables": {
        "vt_insert_test": {
          "ColVindexes": [
            {
              "Col": "id",
              "Name": "id_index"
            }
          ]
        }
      }
    }
  }
}'''

create_tables = [create_vt_insert_test]


def setUpModule():
  logging.debug("in setUpModule")
  try:
    environment.topo_server().setup()

    # start mysql instance external to the test
    setup_procs = [shard_0_master.init_mysql(),
                   shard_0_replica.init_mysql(),
                   shard_1_master.init_mysql(),
                   shard_1_replica.init_mysql()
                  ]
    utils.wait_procs(setup_procs)
    setup_tablets()
  except:
    tearDownModule()
    raise

def tearDownModule():
  global vtgate_server
  logging.debug("in tearDownModule")
  if utils.options.skip_teardown:
    return
  logging.debug("Tearing down the servers and setup")
  utils.vtgate_kill(vtgate_server)
  tablet.kill_tablets([shard_0_master, shard_0_replica, shard_1_master,
                       shard_1_replica])
  teardown_procs = [shard_0_master.teardown_mysql(),
                    shard_0_replica.teardown_mysql(),
                    shard_1_master.teardown_mysql(),
                    shard_1_replica.teardown_mysql(),
                   ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()

  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_replica.remove_tree()
  shard_1_master.remove_tree()
  shard_1_replica.remove_tree()

def setup_tablets():
  global vtgate_server
  global vtgate_port

  # Start up a master mysql and vttablet
  logging.debug("Setting up tablets")
  utils.run_vtctl(['CreateKeyspace', KEYSPACE_NAME])
  utils.run_vtctl(['SetKeyspaceShardingInfo', '-force', KEYSPACE_NAME,
                   'keyspace_id', 'uint64'])
  shard_0_master.init_tablet('master', keyspace=KEYSPACE_NAME, shard='-80')
  shard_0_replica.init_tablet('replica', keyspace=KEYSPACE_NAME, shard='-80')
  shard_1_master.init_tablet('master', keyspace=KEYSPACE_NAME, shard='80-')
  shard_1_replica.init_tablet('replica', keyspace=KEYSPACE_NAME, shard='80-')

  utils.run_vtctl(['RebuildKeyspaceGraph', KEYSPACE_NAME], auto_log=True)

  for t in [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica]:
    t.create_db('vt_test_keyspace')
    for create_table in create_tables:
      t.mquery(shard_0_master.dbname, create_table)
    t.start_vttablet(wait_for_state=None)

  for t in [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica]:
    t.wait_for_vttablet_state('SERVING')

  utils.run_vtctl(['ReparentShard', '-force', KEYSPACE_NAME+'/-80',
                   shard_0_master.tablet_alias], auto_log=True)
  utils.run_vtctl(['ReparentShard', '-force', KEYSPACE_NAME+'/80-',
                   shard_1_master.tablet_alias], auto_log=True)

  utils.run_vtctl(['RebuildKeyspaceGraph', KEYSPACE_NAME],
                   auto_log=True)

  utils.check_srv_keyspace('test_nj', KEYSPACE_NAME,
                           'Partitions(master): -80 80-\n' +
                           'Partitions(replica): -80 80-\n' +
                           'TabletTypes: master,replica')

  vtgate_server, vtgate_port = utils.vtgate_start(schema=schema)


def get_connection(user=None, password=None):
  global vtgate_port
  timeout = 10.0
  conn = None
  vtgate_addrs = {"_vt": ["localhost:%s" % (vtgate_port),]}
  conn = conn_class.connect(vtgate_addrs, timeout,
                            user=user, password=password)
  return conn


class TestVTGateFunctions(unittest.TestCase):
  def setUp(self):
    self.master_tablet = shard_1_master
    self.replica_tablet = shard_1_replica

  def test_query_routing(self):
    """Test VtGate routes queries to the right tablets"""
    try:
      count = 10
      vtgate_conn = get_connection()
      for x in xrange(count):
        vtgate_conn.begin()
        vtgate_conn._execute(
            "insert into vt_insert_test (id, msg) values (%(id)s, %(msg)s)",
            {'msg': 'test %s' % x, 'id': x},
            'master')
        vtgate_conn.commit()
      result = shard_0_master.mquery("vt_test_keyspace", "select * from vt_insert_test")
      self.assertEqual(result, ((1L, 'test 1'), (2L, 'test 2'), (3L, 'test 3'), (5L, 'test 5'), (9L, 'test 9')))
      result = shard_1_master.mquery("vt_test_keyspace", "select * from vt_insert_test")
      self.assertEqual(result, ((0L, 'test 0'), (4L, 'test 4'), (6L, 'test 6'), (7L, 'test 7'), (8L, 'test 8')))
      for x in xrange(count):
        (results, rowcount, lastrowid, fields) = vtgate_conn._execute("select * from vt_insert_test where id = %(id)s", {'id': x}, 'master')
        self.assertEqual(results, [(x, "test %s" % x)])
    except Exception, e:
      logging.debug("failed with error %s, %s" % (str(e), traceback.print_exc()))
      raise


if __name__ == '__main__':
  utils.main()
