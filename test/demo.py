#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""
This program launches and shuts down a demo vitess cluster.
"""

import json
import optparse

import environment
import keyspace_util
import utils


vschema = '''{
  "Keyspaces": {
    "user": {
      "Sharded": true,
      "Vindexes": {
        "user_idx": {
          "Type": "hash_autoinc",
          "Params": {
            "Table": "user_idx",
            "Column": "user_id"
          },
          "Owner": "user"
        },
        "name_user_idx": {
          "Type": "lookup_hash",
          "Params": {
            "Table": "name_user_idx",
            "From": "name",
            "To": "user_id"
          },
          "Owner": "user"
        },
        "music_user_idx": {
          "Type": "lookup_hash_unique_autoinc",
          "Params": {
            "Table": "music_user_idx",
            "From": "music_id",
            "To": "user_id"
          },
          "Owner": "music"
        },
        "keyspace_idx": {
          "Type": "numeric"
        }
      },
      "Classes": {
        "user": {
          "ColVindexes": [
            {
              "Col": "user_id",
              "Name": "user_idx"
            },
            {
              "Col": "name",
              "Name": "name_user_idx"
            }
          ]
        },
        "user_extra": {
          "ColVindexes": [
            {
              "Col": "user_id",
              "Name": "user_idx"
            }
          ]
        },
        "music": {
          "ColVindexes": [
            {
              "Col": "user_id",
              "Name": "user_idx"
            },
            {
              "Col": "music_id",
              "Name": "music_user_idx"
            }
          ]
        },
        "music_extra": {
          "ColVindexes": [
            {
              "Col": "music_id",
              "Name": "music_user_idx"
            },
            {
              "Col": "keyspace_id",
              "Name": "keyspace_idx"
            }
          ]
        }
      },
      "Tables": {
        "user": "user",
        "user_extra": "user_extra",
        "music": "music",
        "music_extra": "music_extra"
      }
    },
    "lookup": {
      "Sharded": false,
      "Tables": {
        "user_idx": "",
        "music_user_idx": "",
        "name_user_idx": ""
      }
    }
  }
}'''

# Verify valid json
json.loads(vschema)

def main():
  parser = optparse.OptionParser(usage="usage: %prog [options]")
  utils.add_options(parser)
  (options, args) = parser.parse_args()
  options.debug = True
  utils.set_options(options)
  env = keyspace_util.TestEnv()
  vtgate_server=None
  try:
    environment.topo_server().setup()
    env.launch(
        "user",
        shards=["-80", "80-"],
        ddls=[
            'create table user(user_id bigint, name varchar(128), primary key(user_id))',
            'create table user_extra(user_id bigint, extra varchar(128), primary key(user_id))',
            'create table music(user_id bigint, music_id bigint, primary key(user_id, music_id))',
            'create table music_extra(music_id bigint, keyspace_id bigint unsigned, primary key(music_id))',
            ],
        )
    env.launch(
        "lookup",
        ddls=[
            'create table user_idx(user_id bigint not null auto_increment, primary key(user_id))',
            'create table name_user_idx(name varchar(128), user_id bigint, primary key(name, user_id))',
            'create table music_user_idx(music_id bigint not null auto_increment, user_id bigint, primary key(music_id))',
            ],
        )
    utils.apply_vschema(vschema)
    vtgate_server, vtgate_port = utils.vtgate_start(cache_ttl='500s')
    utils.Vtctld().start()
    print "vtgate:", vtgate_port
    print "vtctld:", utils.vtctld.port
    utils.pause("the cluster is up, press enter to shut it down...")
  finally:
    utils.vtgate_kill(vtgate_server)
    env.teardown()
    utils.kill_sub_processes()
    utils.remove_tmp_files()
    environment.topo_server().teardown()


if __name__ == '__main__':
  main()
