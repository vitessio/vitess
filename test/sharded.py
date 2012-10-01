#!/usr/bin/python

from optparse import OptionParser
import os
import shutil
import socket
from subprocess import check_call, Popen, CalledProcessError, PIPE
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
      if options.verbose:
        print >> sys.stderr, e, path

def run_test_sharding():

  # set up the following databases:
  # 0000062344: shard 0 master
  # 0000062044: shard 0 slave
  # 0000031981: shard 1 master
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
  time.sleep(1)

  utils.run_vtctl('-force ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/0 /zk/test_nj/vt/tablets/0000062344')
  utils.run_vtctl('-force ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/1 /zk/test_nj/vt/tablets/0000031981')

  utils.zk_check(ping_tablets=True)

  #
  # more to come here
  #

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
