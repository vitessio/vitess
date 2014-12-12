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

from vtdb import dbexceptions
from vtdb import vtgatev3

conn_class = vtgatev3

shard_0_master = tablet.Tablet()
shard_1_master = tablet.Tablet()
lookup_master = tablet.Tablet()

vtgate_server = None
vtgate_port = None

USER_KEYSACE = 'user_keyspace'
LOOKUP_KEYSPACE = 'lookup_keyspace'

create_vt_user = '''create table vt_user (
id bigint,
name varchar(64),
primary key (id)
) Engine=InnoDB'''

create_vt_user_extra = '''create table vt_user_extra (
user_id bigint,
email varchar(64),
primary key (user_id)
) Engine=InnoDB'''

create_vt_music = '''create table vt_music (
user_id bigint,
id bigint,
song varchar(64),
primary key (user_id, id)
) Engine=InnoDB'''

create_vt_music_extra = '''create table vt_music_extra (
music_id bigint,
user_id bigint,
artist varchar(64),
primary key (music_id)
) Engine=InnoDB'''

create_vt_user_idx = '''create table vt_user_idx (
id bigint auto_increment,
primary key (id)
) Engine=InnoDB'''

create_music_user_map = '''create table music_user_map (
music_id bigint auto_increment,
user_id bigint,
primary key (music_id)
) Engine=InnoDB'''

schema = '''{
  "Keyspaces": {
    "user_keyspace": {
      "Sharded": true,
      "Vindexes": {
        "user_index": {
          "Type": "hash",
          "Params": {
            "Table": "vt_user_idx",
            "Column": "id"
          },
          "Owner": "vt_user"
        },
        "music_user_map": {
          "Type": "lookup_hash_unique",
          "Params": {
            "Table": "music_user_map",
            "From": "music_id",
            "To": "user_id"
          },
          "Owner": "vt_music"
        }
      },
      "Tables": {
        "vt_user": {
          "ColVindexes": [
            {
              "Col": "id",
              "Name": "user_index"
            }
          ]
        },
        "vt_user_extra": {
          "ColVindexes": [
            {
              "Col": "user_id",
              "Name": "user_index"
            }
          ]
        },
        "vt_music": {
          "ColVindexes": [
            {
              "Col": "user_id",
              "Name": "user_index"
            },
            {
              "Col": "id",
              "Name": "music_user_map"
            }
          ]
        },
        "vt_music_extra": {
          "ColVindexes": [
            {
              "Col": "music_id",
              "Name": "music_user_map"
            },
            {
              "Col": "user_id",
              "Name": "user_index"
            }
          ]
        }
      }
    },
    "lookup_keyspace": {
      "Sharded": false,
      "Tables": {
        "vt_user_idx": {},
        "music_user_map": {}
      }
    }
  }
}'''

# Verify valid json
json.loads(schema)


def setUpModule():
  logging.debug("in setUpModule")
  try:
    environment.topo_server().setup()

    # start mysql instance external to the test
    setup_procs = [shard_0_master.init_mysql(),
                   shard_1_master.init_mysql(),
                   lookup_master.init_mysql(),
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
  tablet.kill_tablets([shard_0_master, shard_1_master, lookup_master])
  teardown_procs = [shard_0_master.teardown_mysql(),
                    shard_1_master.teardown_mysql(),
                    lookup_master.teardown_mysql(),
                   ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()

  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_1_master.remove_tree()
  lookup_master.remove_tree()

def setup_tablets():
  global vtgate_server
  global vtgate_port

  # Start up a master mysql and vttablet
  logging.debug("Setting up tablets")
  utils.run_vtctl(['CreateKeyspace', USER_KEYSACE])
  utils.run_vtctl(['CreateKeyspace', LOOKUP_KEYSPACE])
  utils.run_vtctl(['SetKeyspaceShardingInfo', '-force', USER_KEYSACE,
                   'keyspace_id', 'uint64'])
  shard_0_master.init_tablet('master', keyspace=USER_KEYSACE, shard='-80')
  shard_1_master.init_tablet('master', keyspace=USER_KEYSACE, shard='80-')
  lookup_master.init_tablet('master', keyspace=LOOKUP_KEYSPACE, shard='0')

  for t in [shard_0_master, shard_1_master]:
    t.create_db('vt_user_keyspace')
    t.mquery('vt_user_keyspace', create_vt_user)
    t.mquery('vt_user_keyspace', create_vt_user_extra)
    t.mquery('vt_user_keyspace', create_vt_music)
    t.mquery('vt_user_keyspace', create_vt_music_extra)
    t.start_vttablet(wait_for_state='SERVING')
    utils.run_vtctl(['SetReadWrite', t.tablet_alias])
  lookup_master.create_db('vt_lookup_keyspace')
  lookup_master.mquery('vt_lookup_keyspace', create_vt_user_idx)
  lookup_master.mquery('vt_lookup_keyspace', create_music_user_map)
  lookup_master.start_vttablet(wait_for_state='SERVING')
  utils.run_vtctl(['SetReadWrite', lookup_master.tablet_alias])

  utils.run_vtctl(['RebuildKeyspaceGraph', USER_KEYSACE], auto_log=True)
  utils.run_vtctl(['RebuildKeyspaceGraph', LOOKUP_KEYSPACE], auto_log=True)

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

  def test_insert_user(self):
    count = 4
    vtgate_conn = get_connection()
    for x in xrange(count):
      i = x+1
      vtgate_conn.begin()
      vtgate_conn._execute(
          "insert into vt_user (name) values (%(name)s)",
          {'name': 'test %s' % i},
          'master')
      vtgate_conn.commit()
    for x in xrange(count):
      i = x+1
      (results, rowcount, lastrowid, fields) = vtgate_conn._execute("select * from vt_user where id = %(id)s", {'id': i}, 'master')
      self.assertEqual(results, [(i, "test %s" % i)])
    vtgate_conn.begin()
    vtgate_conn._execute(
        "insert into vt_user (id, name) values (%(id)s, %(name)s)",
        {'id': 6, 'name': 'test 6'},
        'master')
    vtgate_conn._execute(
        "insert into vt_user (name) values (%(name)s)",
        {'name': 'test 7'},
        'master')
    vtgate_conn.commit()
    result = shard_0_master.mquery("vt_user_keyspace", "select * from vt_user")
    self.assertEqual(result, ((1L, 'test 1'), (2L, 'test 2'), (3L, 'test 3')))
    result = shard_1_master.mquery("vt_user_keyspace", "select * from vt_user")
    self.assertEqual(result, ((4L, 'test 4'), (6L, 'test 6'), (7L, 'test 7')))
    result = lookup_master.mquery("vt_lookup_keyspace", "select * from vt_user_idx")
    self.assertEqual(result, ((1L,), (2L,), (3L,), (4L,), (6L,), (7L,)))

  def test_insert_user_extra(self):
    count = 4
    vtgate_conn = get_connection()
    for x in xrange(count):
      i = x+1
      vtgate_conn.begin()
      vtgate_conn._execute(
          "insert into vt_user_extra (user_id, email) values (%(user_id)s, %(email)s)",
          {'user_id': i, 'email': 'test %s' % i},
          'master')
      vtgate_conn.commit()
    for x in xrange(count):
      i = x+1
      (results, rowcount, lastrowid, fields) = vtgate_conn._execute("select * from vt_user_extra where user_id = %(user_id)s", {'user_id': i}, 'master')
      self.assertEqual(results, [(i, "test %s" % i)])
    result = shard_0_master.mquery("vt_user_keyspace", "select * from vt_user_extra")
    self.assertEqual(result, ((1L, 'test 1'), (2L, 'test 2'), (3L, 'test 3')))
    result = shard_1_master.mquery("vt_user_keyspace", "select * from vt_user_extra")
    self.assertEqual(result, ((4L, 'test 4'),))

  def test_insert_music(self):
    count = 4
    vtgate_conn = get_connection()
    for x in xrange(count):
      i = x+1
      vtgate_conn.begin()
      vtgate_conn._execute(
          "insert into vt_music (user_id, song) values (%(user_id)s, %(song)s)",
          {'user_id': i, 'song': 'test %s' % i},
          'master')
      vtgate_conn.commit()
    for x in xrange(count):
      i = x+1
      (results, rowcount, lastrowid, fields) = vtgate_conn._execute("select * from vt_music where id = %(id)s", {'id': i}, 'master')
      self.assertEqual(results, [(i, i, "test %s" % i)])
    vtgate_conn.begin()
    vtgate_conn._execute(
        "insert into vt_music (user_id, id, song) values (%(user_id)s, %(id)s, %(song)s)",
        {'user_id': 5, 'id': 6, 'song': 'test 6'},
        'master')
    vtgate_conn._execute(
        "insert into vt_music (user_id, song) values (%(user_id)s, %(song)s)",
        {'user_id': 6, 'song': 'test 7'},
        'master')
    vtgate_conn.commit()
    result = shard_0_master.mquery("vt_user_keyspace", "select * from vt_music")
    self.assertEqual(result, ((1L, 1L, 'test 1'), (2L, 2L, 'test 2'), (3L, 3L, 'test 3'), (5L, 6L, 'test 6')))
    result = shard_1_master.mquery("vt_user_keyspace", "select * from vt_music")
    self.assertEqual(result, ((4L, 4L, 'test 4'), (6L, 7L, 'test 7')))
    result = lookup_master.mquery("vt_lookup_keyspace", "select * from music_user_map")
    self.assertEqual(result, ((1L, 1L), (2L, 2L), (3L, 3L), (4L, 4L), (6L, 5L), (7L, 6L)))

  def test_insert_music_extra(self):
    vtgate_conn = get_connection()
    vtgate_conn.begin()
    vtgate_conn._execute(
        "insert into vt_music_extra (music_id, user_id, artist) values (%(music_id)s, %(user_id)s, %(artist)s)",
        {'music_id': 1, 'user_id': 1, 'artist': 'test 1'},
        'master')
    vtgate_conn._execute(
        "insert into vt_music_extra (music_id, artist) values (%(music_id)s, %(artist)s)",
        {'music_id': 6, 'artist': 'test 6'},
        'master')
    vtgate_conn.commit()
    (results, rowcount, lastrowid, fields) = vtgate_conn._execute("select * from vt_music_extra where music_id = %(music_id)s", {'music_id': 6}, 'master')
    self.assertEqual(results, [(6L, 5L, "test 6")])
    result = shard_0_master.mquery("vt_user_keyspace", "select * from vt_music_extra")
    self.assertEqual(result, ((1L, 1L, 'test 1'), (6L, 5L, 'test 6')))
    result = shard_1_master.mquery("vt_user_keyspace", "select * from vt_music_extra")
    self.assertEqual(result, ())

  def test_insert_value_required(self):
    vtgate_conn = get_connection()
    try:
      vtgate_conn.begin()
      with self.assertRaisesRegexp(dbexceptions.DatabaseError, '.*value must be supplied.*'):
        vtgate_conn._execute(
            "insert into vt_user_extra (email) values (%(email)s)",
            {'email': 'test 10'},
            'master')
    finally:
      vtgate_conn.rollback()


if __name__ == '__main__':
  utils.main()
