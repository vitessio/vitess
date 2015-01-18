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
import keyspace_util
import tablet
import utils

from vtdb import cursorv3
from vtdb import dbexceptions
from vtdb import vtgatev3

shard_0_master = tablet.Tablet()
shard_1_master = tablet.Tablet()
lookup_master = tablet.Tablet()

vtgate_server = None
vtgate_port = None
keyspace_env = None

create_vt_user = '''create table vt_user (
id bigint,
name varchar(64),
primary key (id)
) Engine=InnoDB'''

create_vt_user2 = '''create table vt_user2 (
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

create_name_user2_map = '''create table name_user2_map (
name varchar(64),
user2_id bigint,
primary key (name, user2_id)
) Engine=InnoDB'''

create_music_user_map = '''create table music_user_map (
music_id bigint auto_increment,
user_id bigint,
primary key (music_id)
) Engine=InnoDB'''

schema = '''{
  "Keyspaces": {
    "user": {
      "Sharded": true,
      "Vindexes": {
        "user_index": {
          "Type": "hash_autoinc",
          "Params": {
            "Table": "vt_user_idx",
            "Column": "id"
          },
          "Owner": "vt_user"
        },
        "name_user2_map": {
          "Type": "lookup_hash",
          "Params": {
            "Table": "name_user2_map",
            "From": "name",
            "To": "user2_id"
          },
          "Owner": "vt_user2"
        },
        "music_user_map": {
          "Type": "lookup_hash_unique_autoinc",
          "Params": {
            "Table": "music_user_map",
            "From": "music_id",
            "To": "user_id"
          },
          "Owner": "vt_music"
        }
      },
      "Classes": {
        "vt_user": {
          "ColVindexes": [
            {
              "Col": "id",
              "Name": "user_index"
            }
          ]
        },
        "vt_user2": {
          "ColVindexes": [
            {
              "Col": "id",
              "Name": "user_index"
            },
            {
              "Col": "name",
              "Name": "name_user2_map"
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
      },
      "Tables": {
        "vt_user": "vt_user",
        "vt_user2": "vt_user2",
        "vt_user_extra": "vt_user_extra",
        "vt_music": "vt_music",
        "vt_music_extra": "vt_music_extra"
      }
    },
    "lookup": {
      "Sharded": false,
      "Tables": {
        "vt_user_idx": "",
        "music_user_map": "",
        "name_user2_map": ""
      }
    }
  }
}'''

# Verify valid json
json.loads(schema)


def setUpModule():
  global keyspace_env
  global shard_0_master
  global shard_1_master
  global lookup_master
  global vtgate_server
  global vtgate_port
  logging.debug("in setUpModule")

  try:
    environment.topo_server().setup()
    logging.debug("Setting up tablets")
    keyspace_env = keyspace_util.TestEnv()
    keyspace_env.launch(
        "user",
        shards=["-80", "80-"],
        ddls=[
            create_vt_user,
            create_vt_user2,
            create_vt_user_extra,
            create_vt_music,
            create_vt_music_extra,
            ],
        )
    keyspace_env.launch(
        "lookup",
        ddls=[
            create_vt_user_idx,
            create_music_user_map,
            create_name_user2_map,
            ],
        )
    shard_0_master = keyspace_env.tablet_map["user.-80.master"]
    shard_1_master = keyspace_env.tablet_map["user.80-.master"]
    lookup_master = keyspace_env.tablet_map["lookup.0.master"]

    utils.apply_vschema(schema)
    vtgate_server, vtgate_port = utils.vtgate_start()
  except:
    tearDownModule()
    raise

def tearDownModule():
  logging.debug("in tearDownModule")
  if utils.options.skip_teardown:
    return
  logging.debug("Tearing down the servers and setup")
  utils.vtgate_kill(vtgate_server)
  keyspace_env.teardown()

  environment.topo_server().teardown()

  utils.kill_sub_processes()
  utils.remove_tmp_files()


def get_connection(user=None, password=None):
  global vtgate_port
  timeout = 10.0
  return vtgatev3.connect("localhost:%s" % (vtgate_port), timeout,
                            user=user, password=password)


class TestVTGateFunctions(unittest.TestCase):
  def setUp(self):
    self.master_tablet = shard_1_master

  def test_user(self):
    count = 4
    vtgate_conn = get_connection()
    cursor = vtgate_conn.cursor('master')

    # Test insert
    for x in xrange(count):
      i = x+1
      cursor.begin()
      cursor.execute(
          "insert into vt_user (name) values (:name)",
          {'name': 'test %s' % i})
      self.assertEqual((cursor.fetchall(), cursor.rowcount, cursor.lastrowid, cursor.description), ([], 1L, i, []))
      cursor.commit()

    # Test select equal
    for x in xrange(count):
      i = x+1
      cursor.execute("select * from vt_user where id = :id", {'id': i})
      self.assertEqual((cursor.fetchall(), cursor.rowcount, cursor.lastrowid, cursor.description), ([(i, "test %s" % i)], 1L, 0, [('id', 8L), ('name', 253L)]))

    # Test insert with no auto-inc, then auto-inc
    vtgate_conn.begin()
    result = vtgate_conn._execute(
        "insert into vt_user (id, name) values (:id, :name)",
        {'id': 6, 'name': 'test 6'},
        'master')
    self.assertEqual(result, ([], 1L, 0L, []))
    result = vtgate_conn._execute(
        "insert into vt_user (name) values (:name)",
        {'name': 'test 7'},
        'master')
    self.assertEqual(result, ([], 1L, 7L, []))
    vtgate_conn.commit()

    # Verify values in db
    result = shard_0_master.mquery("vt_user", "select * from vt_user")
    self.assertEqual(result, ((1L, 'test 1'), (2L, 'test 2'), (3L, 'test 3')))
    result = shard_1_master.mquery("vt_user", "select * from vt_user")
    self.assertEqual(result, ((4L, 'test 4'), (6L, 'test 6'), (7L, 'test 7')))
    result = lookup_master.mquery("vt_lookup", "select * from vt_user_idx")
    self.assertEqual(result, ((1L,), (2L,), (3L,), (4L,), (6L,), (7L,)))

    # Test IN clause
    result = vtgate_conn._execute("select * from vt_user where id in (:a, :b)", {"a": 1, "b": 4}, 'master')
    result[0].sort()
    self.assertEqual(result, ([(1L, 'test 1'), (4L, 'test 4')], 2L, 0, [('id', 8L), ('name', 253L)]))
    result = vtgate_conn._execute("select * from vt_user where id in (:a, :b)", {"a": 1, "b": 2}, 'master')
    result[0].sort()
    self.assertEqual(result, ([(1L, 'test 1'), (2L, 'test 2')], 2L, 0, [('id', 8L), ('name', 253L)]))

    # Test keyrange
    result = vtgate_conn._execute("select * from vt_user where keyrange('', '\x80')", {}, 'master')
    self.assertEqual(result, ([(1L, 'test 1'), (2L, 'test 2'), (3L, 'test 3')], 3L, 0, [('id', 8L), ('name', 253L)]))
    result = vtgate_conn._execute("select * from vt_user where keyrange('\x80', '')", {}, 'master')
    self.assertEqual(result, ([(4L, 'test 4'), (6L, 'test 6'), (7L, 'test 7')], 3L, 0, [('id', 8L), ('name', 253L)]))

    # Test scatter
    result = vtgate_conn._execute("select * from vt_user", {}, 'master')
    result[0].sort()
    self.assertEqual(result, (
        [(1L, 'test 1'), (2L, 'test 2'), (3L, 'test 3'), (4L, 'test 4'), (6L, 'test 6'), (7L, 'test 7')],
        6L,
        0,
        [('id', 8L), ('name', 253L)],
    ))

    # Test stream
    stream_cursor = vtgate_conn.cursor('master', cursorclass=cursorv3.StreamCursor)
    stream_cursor.execute("select * from vt_user", {})
    self.assertEqual(cursor.description, [('id', 8L), ('name', 253L)])
    rows = []
    for row in stream_cursor:
      rows.append(row)
    rows.sort()
    self.assertEqual(rows, [(1L, 'test 1'), (2L, 'test 2'), (3L, 'test 3'), (4L, 'test 4'), (6L, 'test 6'), (7L, 'test 7')])

    # Test updates
    vtgate_conn.begin()
    result = vtgate_conn._execute(
        "update vt_user set name = :name where id = :id",
        {'id': 1, 'name': 'test one'},
        "master")
    self.assertEqual(result, ([], 1L, 0L, []))
    result = vtgate_conn._execute(
        "update vt_user set name = :name where id = :id",
        {'id': 4, 'name': 'test four'},
        "master")
    self.assertEqual(result, ([], 1L, 0L, []))
    vtgate_conn.commit()
    result = shard_0_master.mquery("vt_user", "select * from vt_user")
    self.assertEqual(result, ((1L, 'test one'), (2L, 'test 2'), (3L, 'test 3')))
    result = shard_1_master.mquery("vt_user", "select * from vt_user")
    self.assertEqual(result, ((4L, 'test four'), (6L, 'test 6'), (7L, 'test 7')))

    # Test deletes
    vtgate_conn.begin()
    result = vtgate_conn._execute(
        "delete from vt_user where id = :id",
        {'id': 1},
        "master")
    self.assertEqual(result, ([], 1L, 0L, []))
    result = vtgate_conn._execute(
        "delete from vt_user where id = :id",
        {'id': 4},
        "master")
    self.assertEqual(result, ([], 1L, 0L, []))
    vtgate_conn.commit()
    result = shard_0_master.mquery("vt_user", "select * from vt_user")
    self.assertEqual(result, ((2L, 'test 2'), (3L, 'test 3')))
    result = shard_1_master.mquery("vt_user", "select * from vt_user")
    self.assertEqual(result, ((6L, 'test 6'), (7L, 'test 7')))
    result = lookup_master.mquery("vt_lookup", "select * from vt_user_idx")
    self.assertEqual(result, ((2L,), (3L,), (6L,), (7L,)))

  def test_user2(self):
    # user2 is for testing non-unique vindexes
    vtgate_conn = get_connection()
    vtgate_conn.begin()
    result = vtgate_conn._execute(
        "insert into vt_user2 (id, name) values (:id, :name)",
        {'id': 1, 'name': 'name1'},
        'master')
    self.assertEqual(result, ([], 1L, 0L, []))
    result = vtgate_conn._execute(
        "insert into vt_user2 (id, name) values (:id, :name)",
        {'id': 7, 'name': 'name1'},
        'master')
    self.assertEqual(result, ([], 1L, 0L, []))
    result = vtgate_conn._execute(
        "insert into vt_user2 (id, name) values (:id, :name)",
        {'id': 2, 'name': 'name2'},
        'master')
    self.assertEqual(result, ([], 1L, 0L, []))
    vtgate_conn.commit()
    result = shard_0_master.mquery("vt_user", "select * from vt_user2")
    self.assertEqual(result, ((1L, 'name1'), (2L, 'name2')))
    result = shard_1_master.mquery("vt_user", "select * from vt_user2")
    self.assertEqual(result, ((7L, 'name1'),))
    result = lookup_master.mquery("vt_lookup", "select * from name_user2_map")
    self.assertEqual(result, (('name1', 1L), ('name1', 7L), ('name2', 2L)))

    # Test select by id
    result = vtgate_conn._execute("select * from vt_user2 where id = :id", {'id': 1}, 'master')
    self.assertEqual(result, ([(1, "name1")], 1L, 0, [('id', 8L), ('name', 253L)]))

    # Test select by lookup
    result = vtgate_conn._execute("select * from vt_user2 where name = :name", {'name': 'name1'}, 'master')
    result[0].sort()
    self.assertEqual(result, ([(1, "name1"), (7, "name1")], 2L, 0, [('id', 8L), ('name', 253L)]))

    # Test IN clause using non-unique vindex
    result = vtgate_conn._execute("select * from vt_user2 where name in ('name1', 'name2')", {}, 'master')
    result[0].sort()
    self.assertEqual(result, ([(1, "name1"), (2, "name2"), (7, "name1")], 3L, 0, [('id', 8L), ('name', 253L)]))
    result = vtgate_conn._execute("select * from vt_user2 where name in ('name1')", {}, 'master')
    result[0].sort()
    self.assertEqual(result, ([(1, "name1"), (7, "name1")], 2L, 0, [('id', 8L), ('name', 253L)]))

    # Test delete
    vtgate_conn.begin()
    result = vtgate_conn._execute(
        "delete from vt_user2 where id = :id",
        {'id': 1},
        "master")
    self.assertEqual(result, ([], 1L, 0L, []))
    result = vtgate_conn._execute(
        "delete from vt_user2 where id = :id",
        {'id': 2},
        "master")
    self.assertEqual(result, ([], 1L, 0L, []))
    vtgate_conn.commit()
    result = shard_0_master.mquery("vt_user", "select * from vt_user2")
    self.assertEqual(result, ())
    result = shard_1_master.mquery("vt_user", "select * from vt_user2")
    self.assertEqual(result, ((7L, 'name1'),))
    result = lookup_master.mquery("vt_lookup", "select * from name_user2_map")
    self.assertEqual(result, (('name1', 7L),))

  def test_user_extra(self):
    # user_extra is for testing unowned functional vindex
    count = 4
    vtgate_conn = get_connection()
    for x in xrange(count):
      i = x+1
      vtgate_conn.begin()
      result = vtgate_conn._execute(
          "insert into vt_user_extra (user_id, email) values (:user_id, :email)",
          {'user_id': i, 'email': 'test %s' % i},
          'master')
      self.assertEqual(result, ([], 1L, 0L, []))
      vtgate_conn.commit()
    for x in xrange(count):
      i = x+1
      result = vtgate_conn._execute("select * from vt_user_extra where user_id = :user_id", {'user_id': i}, 'master')
      self.assertEqual(result, ([(i, "test %s" % i)], 1L, 0, [('user_id', 8L), ('email', 253L)]))
    result = shard_0_master.mquery("vt_user", "select * from vt_user_extra")
    self.assertEqual(result, ((1L, 'test 1'), (2L, 'test 2'), (3L, 'test 3')))
    result = shard_1_master.mquery("vt_user", "select * from vt_user_extra")
    self.assertEqual(result, ((4L, 'test 4'),))

    vtgate_conn.begin()
    result = vtgate_conn._execute(
        "update vt_user_extra set email = :email where user_id = :user_id",
        {'user_id': 1, 'email': 'test one'},
        "master")
    self.assertEqual(result, ([], 1L, 0L, []))
    result = vtgate_conn._execute(
        "update vt_user_extra set email = :email where user_id = :user_id",
        {'user_id': 4, 'email': 'test four'},
        "master")
    self.assertEqual(result, ([], 1L, 0L, []))
    vtgate_conn.commit()
    result = shard_0_master.mquery("vt_user", "select * from vt_user_extra")
    self.assertEqual(result, ((1L, 'test one'), (2L, 'test 2'), (3L, 'test 3')))
    result = shard_1_master.mquery("vt_user", "select * from vt_user_extra")
    self.assertEqual(result, ((4L, 'test four'),))

    vtgate_conn.begin()
    result = vtgate_conn._execute(
        "delete from vt_user_extra where user_id = :user_id",
        {'user_id': 1},
        "master")
    self.assertEqual(result, ([], 1L, 0L, []))
    result = vtgate_conn._execute(
        "delete from  vt_user_extra where user_id = :user_id",
        {'user_id': 4},
        "master")
    self.assertEqual(result, ([], 1L, 0L, []))
    vtgate_conn.commit()
    result = shard_0_master.mquery("vt_user", "select * from vt_user_extra")
    self.assertEqual(result, ((2L, 'test 2'), (3L, 'test 3')))
    result = shard_1_master.mquery("vt_user", "select * from vt_user_extra")
    self.assertEqual(result, ())

  def test_music(self):
    # music is for testing owned lookup index
    count = 4
    vtgate_conn = get_connection()
    for x in xrange(count):
      i = x+1
      vtgate_conn.begin()
      result = vtgate_conn._execute(
          "insert into vt_music (user_id, song) values (:user_id, :song)",
          {'user_id': i, 'song': 'test %s' % i},
          'master')
      self.assertEqual(result, ([], 1L, i, []))
      vtgate_conn.commit()
    for x in xrange(count):
      i = x+1
      result = vtgate_conn._execute("select * from vt_music where id = :id", {'id': i}, 'master')
      self.assertEqual(result, ([(i, i, "test %s" % i)], 1, 0, [('user_id', 8L), ('id', 8L), ('song', 253L)]))
    vtgate_conn.begin()
    result = vtgate_conn._execute(
        "insert into vt_music (user_id, id, song) values (:user_id, :id, :song)",
        {'user_id': 5, 'id': 6, 'song': 'test 6'},
        'master')
    self.assertEqual(result, ([], 1L, 0L, []))
    result = vtgate_conn._execute(
        "insert into vt_music (user_id, song) values (:user_id, :song)",
        {'user_id': 6, 'song': 'test 7'},
        'master')
    self.assertEqual(result, ([], 1L, 7L, []))
    result = vtgate_conn._execute(
        "insert into vt_music (user_id, song) values (:user_id, :song)",
        {'user_id': 6, 'song': 'test 8'},
        'master')
    self.assertEqual(result, ([], 1L, 8L, []))
    vtgate_conn.commit()
    result = shard_0_master.mquery("vt_user", "select * from vt_music")
    self.assertEqual(result, ((1L, 1L, 'test 1'), (2L, 2L, 'test 2'), (3L, 3L, 'test 3'), (5L, 6L, 'test 6')))
    result = shard_1_master.mquery("vt_user", "select * from vt_music")
    self.assertEqual(result, ((4L, 4L, 'test 4'), (6L, 7L, 'test 7'), (6L, 8L, 'test 8')))
    result = lookup_master.mquery("vt_lookup", "select * from music_user_map")
    self.assertEqual(result, ((1L, 1L), (2L, 2L), (3L, 3L), (4L, 4L), (6L, 5L), (7L, 6L), (8L, 6L)))

    vtgate_conn.begin()
    result = vtgate_conn._execute(
        "update vt_music set song = :song where id = :id",
        {'id': 6, 'song': 'test six'},
        "master")
    self.assertEqual(result, ([], 1L, 0L, []))
    result = vtgate_conn._execute(
        "update vt_music set song = :song where id = :id",
        {'id': 7, 'song': 'test seven'},
        "master")
    self.assertEqual(result, ([], 1L, 0L, []))
    vtgate_conn.commit()
    result = shard_0_master.mquery("vt_user", "select * from vt_music")
    self.assertEqual(result, ((1L, 1L, 'test 1'), (2L, 2L, 'test 2'), (3L, 3L, 'test 3'), (5L, 6L, 'test six')))
    result = shard_1_master.mquery("vt_user", "select * from vt_music")
    self.assertEqual(result, ((4L, 4L, 'test 4'), (6L, 7L, 'test seven'), (6L, 8L, 'test 8')))

    vtgate_conn.begin()
    result = vtgate_conn._execute(
        "delete from vt_music where id = :id",
        {'id': 3},
        "master")
    self.assertEqual(result, ([], 1L, 0L, []))
    result = vtgate_conn._execute(
        "delete from vt_music where user_id = :user_id",
        {'user_id': 6},
        "master")
    self.assertEqual(result, ([], 2L, 0L, []))
    vtgate_conn.commit()
    result = shard_0_master.mquery("vt_user", "select * from vt_music")
    self.assertEqual(result, ((1L, 1L, 'test 1'), (2L, 2L, 'test 2'), (5L, 6L, 'test six')))
    result = shard_1_master.mquery("vt_user", "select * from vt_music")
    self.assertEqual(result, ((4L, 4L, 'test 4'),))
    result = lookup_master.mquery("vt_lookup", "select * from music_user_map")
    self.assertEqual(result, ((1L, 1L), (2L, 2L), (4L, 4L), (6L, 5L)))

  def test_music_extra(self):
    # music_extra is for testing unonwed lookup index
    vtgate_conn = get_connection()
    vtgate_conn.begin()
    result = vtgate_conn._execute(
        "insert into vt_music_extra (music_id, user_id, artist) values (:music_id, :user_id, :artist)",
        {'music_id': 1, 'user_id': 1, 'artist': 'test 1'},
        'master')
    self.assertEqual(result, ([], 1L, 0L, []))
    result = vtgate_conn._execute(
        "insert into vt_music_extra (music_id, artist) values (:music_id, :artist)",
        {'music_id': 6, 'artist': 'test 6'},
        'master')
    self.assertEqual(result, ([], 1L, 0L, []))
    vtgate_conn.commit()
    result = vtgate_conn._execute("select * from vt_music_extra where music_id = :music_id", {'music_id': 6}, 'master')
    self.assertEqual(result, ([(6L, 5L, "test 6")], 1, 0, [('music_id', 8L), ('user_id', 8L), ('artist', 253L)]))
    result = shard_0_master.mquery("vt_user", "select * from vt_music_extra")
    self.assertEqual(result, ((1L, 1L, 'test 1'), (6L, 5L, 'test 6')))
    result = shard_1_master.mquery("vt_user", "select * from vt_music_extra")
    self.assertEqual(result, ())

    vtgate_conn.begin()
    result = vtgate_conn._execute(
        "update vt_music_extra set artist = :artist where music_id = :music_id",
        {'music_id': 6, 'artist': 'test six'},
        "master")
    self.assertEqual(result, ([], 1L, 0L, []))
    result = vtgate_conn._execute(
        "update vt_music_extra set artist = :artist where music_id = :music_id",
        {'music_id': 7, 'artist': 'test seven'},
        "master")
    self.assertEqual(result, ([], 0L, 0L, []))
    vtgate_conn.commit()
    result = shard_0_master.mquery("vt_user", "select * from vt_music_extra")
    self.assertEqual(result, ((1L, 1L, 'test 1'), (6L, 5L, 'test six')))

    vtgate_conn.begin()
    result = vtgate_conn._execute(
        "delete from vt_music_extra where music_id = :music_id",
        {'music_id': 6},
        "master")
    self.assertEqual(result, ([], 1L, 0L, []))
    result = vtgate_conn._execute(
        "delete from vt_music_extra where music_id = :music_id",
        {'music_id': 7},
        "master")
    self.assertEqual(result, ([], 0L, 0L, []))
    vtgate_conn.commit()
    result = shard_0_master.mquery("vt_user", "select * from vt_music_extra")
    self.assertEqual(result, ((1L, 1L, 'test 1'),))

  def test_insert_value_required(self):
    vtgate_conn = get_connection()
    try:
      vtgate_conn.begin()
      with self.assertRaisesRegexp(dbexceptions.DatabaseError, '.*value must be supplied.*'):
        vtgate_conn._execute(
            "insert into vt_user_extra (email) values (:email)",
            {'email': 'test 10'},
            'master')
    finally:
      vtgate_conn.rollback()


if __name__ == '__main__':
  utils.main()
