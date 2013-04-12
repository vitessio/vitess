#!/usr/bin/python

import logging
import os
import socket
import tempfile
import time
import datetime

from zk import zkocc

import utils

def setup():
  utils.prog_compile(['zkclient2',
                      ])
  utils.zk_setup()

def teardown():
  if utils.options.skip_teardown:
    return
  utils.zk_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

def _populate_zk():
  utils.zk_wipe()

  utils.run(utils.vtroot+'/bin/zk touch -p /zk/test_nj/zkocc1')
  utils.run(utils.vtroot+'/bin/zk touch -p /zk/test_nj/zkocc2')
  fd = tempfile.NamedTemporaryFile(dir=utils.tmp_root, delete=False)
  filename1 = fd.name
  fd.write("Test data 1")
  fd.close()
  utils.run(utils.vtroot+'/bin/zk cp '+filename1+' /zk/test_nj/zkocc1/data1')

  fd = tempfile.NamedTemporaryFile(dir=utils.tmp_root, delete=False)
  filename2 = fd.name
  fd.write("Test data 2")
  fd.close()
  utils.run(utils.vtroot+'/bin/zk cp '+filename2+' /zk/test_nj/zkocc1/data2')

  fd = tempfile.NamedTemporaryFile(dir=utils.tmp_root, delete=False)
  filename3 = fd.name
  fd.write("Test data 3")
  fd.close()
  utils.run(utils.vtroot+'/bin/zk cp '+filename3+' /zk/test_nj/zkocc1/data3')

def _check_zk_output(cmd, expected):
  # directly for sanity
  out, err = utils.run(utils.vtroot+'/bin/zk ' + cmd, trap_output=True)
  if out != expected:
    raise utils.TestError('unexpected direct zk output: ', cmd, "is", out, "but expected", expected)

  # using zkocc
  out, err = utils.run(utils.vtroot+'/bin/zk --zk.zkocc-addr=localhost:%u %s' % (utils.zkocc_port_base, cmd), trap_output=True)
  if out != expected:
    raise utils.TestError('unexpected zk zkocc output: ', cmd, "is", out, "but expected", expected)

  utils.debug("Matched: " + out)

def _format_time(timeFromBson):
  (tz, val) = timeFromBson
  t = datetime.datetime.fromtimestamp(val/1000)
  return t.strftime("%Y-%m-%d %H:%M:%S")

@utils.test_case
def run_test_zkocc():
  _populate_zk()

  # preload the test_nj cell
  zkocc_14850 = utils.zkocc_start(extra_params=['-connect-timeout=2s', '-cache-refresh-interval=1s'])
  time.sleep(1)

  # create a python client. The first address is bad, will test the retry logic
  bad_port = utils.reserve_ports(3)
  zkocc_client = zkocc.ZkOccConnection("localhost:%u,localhost:%u,localhost:%u" % (bad_port, utils.zkocc_port_base, bad_port+1), "test_nj", 30)
  zkocc_client.dial()

  # test failure for a python client that cannot connect
  bad_zkocc_client = zkocc.ZkOccConnection("localhost:%u,localhost:%u" % (bad_port+2, bad_port), "test_nj", 30)
  try:
    bad_zkocc_client.dial()
    raise utils.TestError('exception expected')
  except zkocc.ZkOccError as e:
    if str(e) != "Cannot dial to any server":
      raise utils.TestError('Unexpected exception: ', str(e))
  logging.getLogger().setLevel(logging.ERROR)
  try:
    bad_zkocc_client.get("/zk/test_nj/zkocc1/data1")
    raise utils.TestError('exception expected')
  except zkocc.ZkOccError as e:
    if str(e) != "Cannot dial to any server":
      raise utils.TestError('Unexpected exception: ', str(e))
  logging.getLogger().setLevel(logging.WARNING)

  # get test
  out, err = utils.run(utils.vtroot+'/bin/zkclient2 -server localhost:%u /zk/test_nj/zkocc1/data1' % utils.zkocc_port_base, trap_output=True)
  if err != "/zk/test_nj/zkocc1/data1 = Test data 1 (NumChildren=0, Version=0, Cached=false, Stale=false)\n":
    raise utils.TestError('unexpected get output: ', err)
  zkNode = zkocc_client.get("/zk/test_nj/zkocc1/data1")
  if (zkNode['Data'] != "Test data 1" or \
      zkNode['Stat']['NumChildren'] != 0 or \
      zkNode['Stat']['Version'] != 0 or \
      zkNode['Cached'] != True or \
      zkNode['Stale'] != False):
    raise utils.TestError('unexpected zkocc_client.get output: ', zkNode)

  # getv test
  out, err = utils.run(utils.vtroot+'/bin/zkclient2 -server localhost:%u /zk/test_nj/zkocc1/data1 /zk/test_nj/zkocc1/data2 /zk/test_nj/zkocc1/data3' % utils.zkocc_port_base, trap_output=True)
  if err != """[0] /zk/test_nj/zkocc1/data1 = Test data 1 (NumChildren=0, Version=0, Cached=true, Stale=false)
[1] /zk/test_nj/zkocc1/data2 = Test data 2 (NumChildren=0, Version=0, Cached=false, Stale=false)
[2] /zk/test_nj/zkocc1/data3 = Test data 3 (NumChildren=0, Version=0, Cached=false, Stale=false)
""":
    raise utils.TestError('unexpected getV output: ', err)
  zkNodes = zkocc_client.getv(["/zk/test_nj/zkocc1/data1", "/zk/test_nj/zkocc1/data2", "/zk/test_nj/zkocc1/data3"])
  if (zkNodes['Nodes'][0]['Data'] != "Test data 1" or \
      zkNodes['Nodes'][0]['Stat']['NumChildren'] != 0 or \
      zkNodes['Nodes'][0]['Stat']['Version'] != 0 or \
      zkNodes['Nodes'][0]['Cached'] != True or \
      zkNodes['Nodes'][0]['Stale'] != False or \
      zkNodes['Nodes'][1]['Data'] != "Test data 2" or \
      zkNodes['Nodes'][1]['Stat']['NumChildren'] != 0 or \
      zkNodes['Nodes'][1]['Stat']['Version'] != 0 or \
      zkNodes['Nodes'][1]['Cached'] != True or \
      zkNodes['Nodes'][1]['Stale'] != False or \
      zkNodes['Nodes'][2]['Data'] != "Test data 3" or \
      zkNodes['Nodes'][2]['Stat']['NumChildren'] != 0 or \
      zkNodes['Nodes'][2]['Stat']['Version'] != 0 or \
      zkNodes['Nodes'][2]['Cached'] != True or \
      zkNodes['Nodes'][2]['Stale'] != False):
    raise utils.TestError('unexpected zkocc_client.getv output: ', zkNodes)

  # children test
  out, err = utils.run(utils.vtroot+'/bin/zkclient2 -server localhost:%u -mode children /zk/test_nj' % utils.zkocc_port_base, trap_output=True)
  if err != """Path = /zk/test_nj
Child[0] = zkocc1
Child[1] = zkocc2
NumChildren = 2
CVersion = 4
Cached = false
Stale = false
""":
    raise utils.TestError('unexpected children output: ', err)

  # zk command tests
  _check_zk_output("cat /zk/test_nj/zkocc1/data1", "Test data 1")
  _check_zk_output("ls -l /zk/test_nj/zkocc1", """total: 3
-rw-rw-rw- zk zk       11  %s data1
-rw-rw-rw- zk zk       11  %s data2
-rw-rw-rw- zk zk       11  %s data3
""" % (_format_time(zkNodes['Nodes'][0]['Stat']['MTime']),
       _format_time(zkNodes['Nodes'][1]['Stat']['MTime']),
       _format_time(zkNodes['Nodes'][2]['Stat']['MTime'])))

  # test /zk/local is not resolved and rejected
  out, err = utils.run(utils.vtroot+'/bin/zkclient2 -server localhost:%u /zk/local/zkocc1/data1' % utils.zkocc_port_base, trap_output=True, raise_on_error=False)
  if "zkocc: cannot resolve local cell" not in err:
    raise utils.TestError('unexpected get output, not local cell error: ', err)

  # start a background process to query the same value over and over again
  # while we kill the zk server and restart it
  outfd = tempfile.NamedTemporaryFile(dir=utils.tmp_root, delete=False)
  filename = outfd.name
  querier = utils.run_bg('/bin/bash -c "while true ; do '+utils.vtroot+'/bin/zkclient2 -server localhost:%u /zk/test_nj/zkocc1/data1 ; sleep 0.1 ; done"' % utils.zkocc_port_base, stderr=outfd.file)
  outfd.close()
  time.sleep(1)

  # kill zk server, sleep a bit, restart zk server, sleep a bit
  utils.run(utils.vtroot+'/bin/zkctl -zk.cfg 1@'+utils.hostname+':%u:%u:%u shutdown' % (utils.zk_port_base, utils.zk_port_base+1, utils.zk_port_base+2))
  time.sleep(3)
  utils.run(utils.vtroot+'/bin/zkctl -zk.cfg 1@'+utils.hostname+':%u:%u:%u start' % (utils.zk_port_base, utils.zk_port_base+1, utils.zk_port_base+2))
  time.sleep(3)

  utils.kill_sub_process(querier)

  utils.debug("Checking " + filename)
  fd = open(filename, "r")
  state = 0
  for line in fd:
    if line == "/zk/test_nj/zkocc1/data1 = Test data 1 (NumChildren=0, Version=0, Cached=true, Stale=false)\n":
      stale = False
    elif line == "/zk/test_nj/zkocc1/data1 = Test data 1 (NumChildren=0, Version=0, Cached=true, Stale=true)\n":
      stale = True
    else:
      raise utils.TestError('unexpected line: ', line)
    if state == 0:
      if stale:
        state = 1
    elif state == 1:
      if not stale:
        state = 2
    else:
      if stale:
        raise utils.TestError('unexpected stale state')
  if state != 2:
    raise utils.TestError('unexpected ended stale state')
  fd.close()

  utils.zkocc_kill(zkocc_14850)

  # check that after the server is gone, the python client fails correctly
  logging.getLogger().setLevel(logging.ERROR)
  try:
    zkocc_client.get("/zk/test_nj/zkocc1/data1")
    raise utils.TestError('exception expected')
  except zkocc.ZkOccError as e:
    if str(e) != "Cannot dial to any server":
      raise utils.TestError('Unexpected exception: ', str(e))
  logging.getLogger().setLevel(logging.WARNING)

@utils.test_case
def run_test_zkocc_qps():
  _populate_zk()

  # preload the test_nj cell
  zkocc_14850 = utils.zkocc_start()

  qpser = utils.run_bg(utils.vtroot+'/bin/zkclient2 -server localhost:%u -mode qps /zk/test_nj/zkocc1/data1 /zk/test_nj/zkocc1/data2' % utils.zkocc_port_base)
  time.sleep(10)
  utils.kill_sub_process(qpser)

  # get the zkocc vars, make sure we have what we need
  v = utils.get_vars(utils.zkocc_port_base)
  if v['ZkReader']['test_nj']['State']['Current'] != 'Connected':
    raise utils.TestError('invalid zk global state: ', v['ZkReader']['test_nj']['State']['Current'])
  if v['ZkReader']['test_nj']['State']['DurationConnected'] < 9e9:
    raise utils.TestError('not enough time in Connected state', v['ZkReader']['test_nj']['State']['DurationConnected'])

  # some checks on performance / stats
  # a typical workstation will do 15k QPS, check we have more than 3k
  rpcCalls = v['ZkReader']['RpcCalls']
  if rpcCalls < 30000:
    raise utils.TestError('QPS is too low: %u < 30000', rpcCalls / 10)
  cacheReads = v['ZkReader']['test_nj']['CacheReads']
  if cacheReads < 30000:
    raise utils.TestError('Cache QPS is too low: %u < 30000', cacheReads / 10)
  totalCacheReads = v['ZkReader']['total']['CacheReads']
  if cacheReads != totalCacheReads:
    raise utils.TestError('Rollup stats are wrong: %u != %u', cacheReads,
                          totalCacheReads)
  if v['ZkReader']['UnknownCellErrors'] != 0:
    raise utils.TestError('unexpected UnknownCellErrors', v['ZkReader']['UnknownCellErrors'])

  utils.zkocc_kill(zkocc_14850)

def run_all():
  run_test_zkocc()
  run_test_zkocc_qps()

def main():
  args = utils.get_args()

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
