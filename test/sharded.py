#!/usr/bin/python

import json
from optparse import OptionParser
import os
import shutil
import socket
from subprocess import check_call, Popen, CalledProcessError, PIPE
import sys
import tempfile
import time
import datetime

from zk import zkocc

import utils

vttop = os.environ['VTTOP']
vtroot = os.environ['VTROOT']
hostname = socket.gethostname()

def setup():
  utils.prog_compile(['mysqlctl',
                      'zkocc',
                      ])
  utils.zk_setup()

  setup_procs = [
      utils.run_bg(vtroot+'/bin/mysqlctl -tablet-uid 62344 -port 6700 -mysql-port 3700 init'),
      utils.run_bg(vtroot+'/bin/mysqlctl -tablet-uid 62044 -port 6701 -mysql-port 3701 init'),
      utils.run_bg(vtroot+'/bin/mysqlctl -tablet-uid 41983 -port 6702 -mysql-port 3702 init'),
      utils.run_bg(vtroot+'/bin/mysqlctl -tablet-uid 31981 -port 6703 -mysql-port 3703 init'),
      ]
  utils.wait_procs(setup_procs)

def teardown():
  if utils.options.skip_teardown:
    return

  teardown_procs = [utils.run_bg(vtroot+'/bin/mysqlctl -tablet-uid %u -force teardown' % x) for x in (62344, 62044, 41983, 31981)]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  utils.zk_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  for path in ('/vt/vt_0000062344', '/vt/vt_0000062044', '/vt/vt_0000031981', '/vt/vt_0000041983'):
    try:
      shutil.rmtree(path)
    except OSError as e:
      if utils.options.verbose:
        print >> sys.stderr, e, path

create_vt_select_test = '''create table vt_select_test (
id bigint not null,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

def check_rows(to_look_for, driver="vtdb"):
  out, err = utils.vttablet_query(0, "/zk/test_nj/vt/ns/test_keyspace/master", "select id, msg from vt_select_test", driver=driver, verbose=True)
  for pattern in to_look_for:
    if err.find(pattern) == -1:
      print "vttablet_query returned:"
      print out
      print err
      raise utils.TestError('wrong vtclient2 output, missing: ' + pattern)
  if utils.options.verbose:
    print err

def run_test_sharding():

  # set up the following databases:
  # 0000062344: shard 0 master 0000000000000000 - 8000000000000000
  # 0000062044: shard 0 slave
  # 0000031981: shard 1 master 8000000000000000 - FFFFFFFFFFFFFFFF
  # 0000041983: shard 1 slave

  utils.run_vtctl('-force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')

  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace 0 master')
  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062044 localhost 3701 6701 test_keyspace 0 replica /zk/global/vt/keyspaces/test_keyspace/shards/0/test_nj-0000062344')
  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000031981 localhost 3702 6702 test_keyspace 1 master')
  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000041983 localhost 3703 6703 test_keyspace 1 replica /zk/global/vt/keyspaces/test_keyspace/shards/1/test_nj-0000031981')

  utils.run_vtctl('RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/0')
  utils.run_vtctl('RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/1')

  utils.run_vtctl('RebuildKeyspace /zk/global/vt/keyspaces/test_keyspace')

  # run checks now before we start the tablets
  utils.zk_check()

  # create databases and schema so tablets are good to go
  for port in [62344, 62044, 31981, 41983]:
    utils.mysql_query(port, '', 'drop database if exists vt_test_keyspace')
    utils.mysql_query(port, '', 'create database vt_test_keyspace')
    utils.mysql_query(port, 'vt_test_keyspace', create_vt_select_test)

  # start the tablets
  agents = [
      utils.run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log'),
      utils.run_bg(vtroot+'/bin/vttablet -port 6701 -tablet-path /zk/test_nj/vt/tablets/0000062044 -logfile /vt/vt_0000062044/vttablet.log'),
      utils.run_bg(vtroot+'/bin/vttablet -port 6702 -tablet-path /zk/test_nj/vt/tablets/0000031981 -logfile /vt/vt_0000031981/vttablet.log'),
      utils.run_bg(vtroot+'/bin/vttablet -port 6703 -tablet-path /zk/test_nj/vt/tablets/0000041983 -logfile /vt/vt_0000041983/vttablet.log'),
      ]

  utils.run(vtroot+'/bin/zk wait -e /zk/test_nj/vt/tablets/0000062344/pid', stdout=utils.devnull)
  utils.run(vtroot+'/bin/zk wait -e /zk/test_nj/vt/tablets/0000062044/pid', stdout=utils.devnull)
  utils.run(vtroot+'/bin/zk wait -e /zk/test_nj/vt/tablets/0000031981/pid', stdout=utils.devnull)
  utils.run(vtroot+'/bin/zk wait -e /zk/test_nj/vt/tablets/0000041983/pid', stdout=utils.devnull)

  # start zkocc, we'll use it later
  zkocc = utils.run_bg(vtroot+'/bin/zkocc -port=14850 test_nj')

  utils.run_vtctl('-force ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/0 /zk/test_nj/vt/tablets/0000062344')
  utils.run_vtctl('-force ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/1 /zk/test_nj/vt/tablets/0000031981')

  # FIXME(alainjobart) fix the test_nj serving graph and use it:
  # - it needs to have KeyRange(Start, End) setup
  #   (I may be missing this setting earlier in the setup)
  #   (I am saving the original file into /vt/tmp/old_test_nj_test_keyspace)
  # - we should use the 'local' cell, not a given cell, but the logic
  #   to create it is not there yet I think.
  fd = tempfile.NamedTemporaryFile(dir=utils.tmp_root, delete=False)
  filename = fd.name
  zk_srv_keyspace = {
      "Shards": [
          {
              "KeyRange": {
                  "Start": "",
                  "End": "8000000000000000"
                  },
              "AddrsByType": {
                  "master": {
                      "entries": [
                          {
                              "uid": 62344,
                              "host": "localhost",
                              "port": 0,
                              "named_port_map": {
                                  "_mysql": 3700,
                                  "_vtocc": 6700
                                  }
                              }
                          ]
                      },
                  "replica": {
                      "entries": [
                          {
                              "uid": 62044,
                              "host": "localhost",
                              "port": 0,
                              "named_port_map": {
                                  "_mysql": 3701,
                                  "_vtocc": 6701
                                  }
                              }
                          ]
                      }
                  },
              "ReadOnly": False
              },
          {
              "KeyRange": {
                  "Start": "8000000000000000",
                  "End": "FFFFFFFFFFFFFFFF"
                  },
              "AddrsByType": {
                  "master": {
                      "entries": [
                          {
                              "uid": 31981,
                              "host": "localhost",
                              "port": 0,
                              "named_port_map": {
                                  "_mysql": 3702,
                                  "_vtocc": 6702
                                  }
                              }
                          ]
                      },
                  "replica": {
                      "entries": [
                          {
                              "uid": 41983,
                              "host": "localhost",
                              "port": 0,
                              "named_port_map": {
                                  "_mysql": 3703,
                                  "_vtocc": 6703
                                  }
                              }
                          ]
                      }
                  },
              "ReadOnly": False
              },
          ],
      "TabletTypes": None
      }
  json.dump(zk_srv_keyspace, fd)
  fd.close()
  utils.run(vtroot+'/bin/zk cp /zk/test_nj/vt/ns/test_keyspace /vt/tmp/old_test_nj_test_keyspace')
  utils.run(vtroot+'/bin/zk cp '+filename+' /zk/test_nj/vt/ns/test_keyspace')

  # insert some values directly
  # FIXME(alainjobart) these values don't match the shard map
  utils.mysql_write_query(62344, 'vt_test_keyspace', "insert into vt_select_test (id, msg) values (1, 'test 1')")
  utils.mysql_write_query(31981, 'vt_test_keyspace', "insert into vt_select_test (id, msg) values (10, 'test 10')")

  utils.zk_check(ping_tablets=True)

  utils.pause("Before the sql scatter query")

  # note the order of the rows is not guaranteed, as the go routines
  # doing the work can go out of order
  check_rows(["Index\tid\tmsg",
              "1\ttest 1",
              "10\ttest 10"])

  # write a value, re-read them all
  out, err = utils.vttablet_query(3803, "/zk/test_nj/vt/ns/test_keyspace/master", "insert into vt_select_test (id, msg) values (2, 'test 2')", driver="vtdb", verbose=True)
  print out
  print err

  check_rows(["Index\tid\tmsg",
              "1\ttest 1",
              "2\ttest 2",
              "10\ttest 10"])

  # make sure the '2' value was written on first shard
  rows = utils.mysql_query(62344, 'vt_test_keyspace', "select id, msg from vt_select_test order by id")
  if (len(rows) != 2 or \
        rows[0][0] != 1 or \
        rows[1][0] != 2):
    print "mysql_query returned:", rows
    raise utils.TestError('wrong mysql_query output')

  utils.pause("After db writes")

  # now use zkocc or streaming or both for the same query
  check_rows(["Index\tid\tmsg",
              "1\ttest 1",
              "2\ttest 2",
              "10\ttest 10"],
             driver="vtdb-zkocc")
# FIXME(alainjobart) the streaming drivers are not working,
# will fix in a different check-in
#  check_rows(["Index\tid\tmsg",
#              "1\ttest 1",
#              "2\ttest 2",
#              "10\ttest 10"],
#             driver="vtdb-streaming")
#  check_rows(["Index\tid\tmsg",
#              "1\ttest 1",
#              "2\ttest 2",
#              "10\ttest 10"],
#             driver="vtdb-zkocc-streaming")

  utils.kill_sub_process(zkocc)
  for agent in agents:
    utils.kill_sub_process(agent)

def run_all():
  run_test_sharding()

def main():
  parser = OptionParser()
  parser.add_option('-v', '--verbose', action='store_true')
  parser.add_option('-d', '--debug', action='store_true')
  parser.add_option('--skip-teardown', action='store_true')
  (utils.options, args) = parser.parse_args()

  if not args:
    args = ['run_all']

  try:
    if args[0] != 'teardown':
      setup()
      if args[0] != 'setup':
        for arg in args:
          globals()[arg]()
          print "GREAT SUCCESS"
  except KeyboardInterrupt:
    pass
  except utils.Break:
    utils.options.skip_teardown = True
  finally:
    teardown()


if __name__ == '__main__':
  main()
