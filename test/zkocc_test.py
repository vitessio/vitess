#!/usr/bin/env python
import datetime
import logging
import re
import tempfile
import time
import unittest

import tablet
import utils
from zk import zkocc


def setUpModule():
  try:
    utils.zk_setup()

  except:
    tearDownModule()
    raise


def tearDownModule():
  if utils.options.skip_teardown:
    return

  utils.zk_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()


class TopoOccTest(unittest.TestCase):
  def setUp(self):
    utils.zk_wipe()
    self.zkocc_server = utils.zkocc_start()
    self.vttopo_server = utils.vttopo_start()
    self.topo = zkocc.ZkOccConnection("localhost:%u" % utils.zkocc_port_base, 'test_nj', 30)
    self.topo.dial()

  def tearDown(self):
    utils.zkocc_kill(self.zkocc_server)
    utils.vttopo_kill(self.vttopo_server)

  def rebuild(self):
    utils.run_vtctl('RebuildShardGraph /zk/global/vt/keyspaces/test_keyspace/shards/0', auto_log=True)
    utils.run_vtctl('RebuildKeyspaceGraph /zk/global/vt/keyspaces/*', auto_log=True)

  def test_get_srv_keyspace_names(self):
    utils.run_vtctl('CreateKeyspace test_keyspace1')
    utils.run_vtctl('CreateKeyspace test_keyspace2')
    t1 = tablet.Tablet(tablet_uid=1, cell="nj")
    t1.init_tablet("master", "test_keyspace1", "0")
    utils.run_vtctl('UpdateTabletAddrs %s -mysql-ip-addr 127.0.0.1:%s -secure-addr 127.0.0.1:%s' % (t1.tablet_alias, t1.mysql_port, t1.port + 500))
    t2 = tablet.Tablet(tablet_uid=2, cell="nj")
    t2.init_tablet("master", "test_keyspace2", "0")
    utils.run_vtctl('UpdateTabletAddrs %s -mysql-ip-addr 127.0.0.1:%s -secure-addr 127.0.0.1:%s' % (t2.tablet_alias, t2.mysql_port, t2.port + 500))
    utils.run_vtctl('RebuildKeyspaceGraph /zk/global/vt/keyspaces/*', auto_log=True)
    self.assertItemsEqual(self.topo.get_srv_keyspace_names('local'), ["test_keyspace1", "test_keyspace2"])

    # zkocc API test
    utils.prog_compile(['zkclient2'])
    out, err = utils.run(utils.vtroot+'/bin/zkclient2 -server localhost:%u -mode getSrvKeyspaceNames test_nj' % utils.zkocc_port_base, trap_output=True)
    self.assertEqual(err, "KeyspaceName[0] = test_keyspace1\n" +
                          "KeyspaceName[1] = test_keyspace2\n")

    # vttopo API test
    out, err = utils.run(utils.vtroot+'/bin/zkclient2 -server localhost:%u -mode getSrvKeyspaceNames test_nj' % utils.vttopo_port_base, trap_output=True)
    self.assertEqual(err, "KeyspaceName[0] = test_keyspace1\n" +
                          "KeyspaceName[1] = test_keyspace2\n")

  def test_get_srv_keyspace(self):
    utils.run_vtctl('CreateKeyspace test_keyspace')
    t = tablet.Tablet(tablet_uid=1, cell="nj")
    t.init_tablet("master", "test_keyspace", "0")
    utils.run_vtctl('UpdateTabletAddrs %s -mysql-ip-addr 127.0.0.1:%s -secure-addr 127.0.0.1:%s' % (t.tablet_alias, t.mysql_port, t.port + 500))
    self.rebuild()
    reply = self.topo.get_srv_keyspace("test_nj", "test_keyspace")
    self.assertEqual(reply['TabletTypes'], ['master'])

    # zkocc API test
    utils.prog_compile(['zkclient2'])
    out, err = utils.run(utils.vtroot+'/bin/zkclient2 -server localhost:%u -mode getSrvKeyspace test_nj test_keyspace' % utils.zkocc_port_base, trap_output=True)
    self.assertEqual(err, "TabletType[0] = master\n")

    # vttopo API test
    out, err = utils.run(utils.vtroot+'/bin/zkclient2 -server localhost:%u -mode getSrvKeyspace test_nj test_keyspace' % utils.vttopo_port_base, trap_output=True)
    self.assertEqual(err, "TabletType[0] = master\n")

  def test_get_srv_keyspace_local(self):
    utils.run_vtctl('CreateKeyspace test_keyspace')
    t = tablet.Tablet(tablet_uid=1, cell="nj")
    t.init_tablet("master", "test_keyspace", "0")
    utils.run_vtctl('UpdateTabletAddrs %s -mysql-ip-addr 127.0.0.1:%s -secure-addr 127.0.0.1:%s' % (t.tablet_alias, t.mysql_port, t.port + 500))
    self.rebuild()
    reply = self.topo.get_srv_keyspace("local", "test_keyspace")
    self.assertEqual(reply['TabletTypes'], ['master'])

  def test_get_end_points(self):
    utils.run_vtctl('CreateKeyspace test_keyspace')
    t = tablet.Tablet(tablet_uid=1, cell="nj")
    t.init_tablet("master", "test_keyspace", "0")
    t.update_addrs(mysql_ip_addr="127.0.0.1:%s" % t.mysql_port, secure_addr="localhost:%s" % (t.port + 500))
    self.rebuild()
    self.assertEqual(len(self.topo.get_end_points("test_nj", "test_keyspace", "0", "master")['Entries']), 1)

    # zkocc API test
    utils.prog_compile(['zkclient2'])
    out, err = utils.run(utils.vtroot+'/bin/zkclient2 -server localhost:%u -mode getEndPoints test_nj test_keyspace 0 master' % utils.zkocc_port_base, trap_output=True)
    self.assertEqual(err, "Entry[0] = 1 localhost\n")

    # vttopo API test
    out, err = utils.run(utils.vtroot+'/bin/zkclient2 -server localhost:%u -mode getEndPoints test_nj test_keyspace 0 master' % utils.vttopo_port_base, trap_output=True)
    self.assertEqual(err, "Entry[0] = 1 localhost\n")


def _format_time(timeFromBson):
  (tz, val) = timeFromBson
  t = datetime.datetime.fromtimestamp(val/1000)
  return t.strftime("%Y-%m-%d %H:%M:%S")


class TestZkocc(unittest.TestCase):
  longMessage = True
  def setUp(self):
    utils.zk_wipe()
    utils.run(utils.vtroot+'/bin/zk touch -p /zk/test_nj/vt/zkocc1')
    utils.run(utils.vtroot+'/bin/zk touch -p /zk/test_nj/vt/zkocc2')
    fd = tempfile.NamedTemporaryFile(dir=utils.tmp_root, delete=False)
    filename1 = fd.name
    fd.write("Test data 1")
    fd.close()
    utils.run(utils.vtroot+'/bin/zk cp '+filename1+' /zk/test_nj/vt/zkocc1/data1')

    fd = tempfile.NamedTemporaryFile(dir=utils.tmp_root, delete=False)
    filename2 = fd.name
    fd.write("Test data 2")
    fd.close()
    utils.run(utils.vtroot+'/bin/zk cp '+filename2+' /zk/test_nj/vt/zkocc1/data2')

    fd = tempfile.NamedTemporaryFile(dir=utils.tmp_root, delete=False)
    filename3 = fd.name
    fd.write("Test data 3")
    fd.close()
    utils.run(utils.vtroot+'/bin/zk cp '+filename3+' /zk/test_nj/vt/zkocc1/data3')

  def _check_zk_output(self, cmd, expected):
    # directly for sanity
    out, err = utils.run(utils.vtroot+'/bin/zk ' + cmd, trap_output=True)
    self.assertEqualNormalized(out, expected, 'unexpected direct zk output')

    # using zkocc
    out, err = utils.run(utils.vtroot+'/bin/zk --zk.zkocc-addr=localhost:%u %s' % (utils.zkocc_port_base, cmd), trap_output=True)
    self.assertEqualNormalized(out, expected, 'unexpected zk zkocc output')
    logging.debug("Matched: %s", out)


  def assertEqualNormalized(self, actual, expected, msg=None):
    self.assertEqual(re.sub(r'\s+', ' ', actual).strip(), re.sub(r'\s+', ' ', expected).strip(), msg)

  def test_zkocc(self):
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
        raise
    level = logging.getLogger().getEffectiveLevel()
    logging.getLogger().setLevel(logging.ERROR)

    # FIXME(ryszard): This can be changed into a self.assertRaises.
    try:
      bad_zkocc_client.get("/zk/test_nj/vt/zkocc1/data1")
      self.fail('exception expected')
    except zkocc.ZkOccError as e:
      if str(e) != "Cannot dial to any server":
        raise

    logging.getLogger().setLevel(level)

    # get test
    utils.prog_compile(['zkclient2'])
    out, err = utils.run(utils.vtroot+'/bin/zkclient2 -server localhost:%u /zk/test_nj/vt/zkocc1/data1' % utils.zkocc_port_base, trap_output=True)
    self.assertEqual(err, "/zk/test_nj/vt/zkocc1/data1 = Test data 1 (NumChildren=0, Version=0, Cached=false, Stale=false)\n")

    zk_data = zkocc_client.get("/zk/test_nj/vt/zkocc1/data1")
    self.assertDictContainsSubset({'Data': "Test data 1",
                                   'Cached': True,
                                   'Stale': False,},
                                  zk_data)
    self.assertDictContainsSubset({'NumChildren': 0, 'Version': 0}, zk_data['Stat'])

    # getv test
    out, err = utils.run(utils.vtroot+'/bin/zkclient2 -server localhost:%u /zk/test_nj/vt/zkocc1/data1 /zk/test_nj/vt/zkocc1/data2 /zk/test_nj/vt/zkocc1/data3' % utils.zkocc_port_base, trap_output=True)
    self.assertEqualNormalized(err, """[0] /zk/test_nj/vt/zkocc1/data1 = Test data 1 (NumChildren=0, Version=0, Cached=true, Stale=false)
  [1] /zk/test_nj/vt/zkocc1/data2 = Test data 2 (NumChildren=0, Version=0, Cached=false, Stale=false)
  [2] /zk/test_nj/vt/zkocc1/data3 = Test data 3 (NumChildren=0, Version=0, Cached=false, Stale=false)
  """)
    zk_data = zkocc_client.getv(["/zk/test_nj/vt/zkocc1/data1", "/zk/test_nj/vt/zkocc1/data2", "/zk/test_nj/vt/zkocc1/data3"])['Nodes']
    self.assertEqual(len(zk_data), 3)
    for i, d in enumerate(zk_data):
      self.assertEqual(d['Data'], 'Test data %s' % (i + 1))
      self.assertTrue(d['Cached'])
      self.assertFalse(d['Stale'])
      self.assertDictContainsSubset({'NumChildren': 0, 'Version': 0}, d['Stat'])

    # children test
    out, err = utils.run(utils.vtroot+'/bin/zkclient2 -server localhost:%u -mode children /zk/test_nj/vt' % utils.zkocc_port_base, trap_output=True)
    self.assertEqualNormalized(err, """Path = /zk/test_nj/vt
  Child[0] = zkocc1
  Child[1] = zkocc2
  NumChildren = 2
  CVersion = 2
  Cached = false
  Stale = false
  """)

    # zk command tests
    self._check_zk_output("cat /zk/test_nj/vt/zkocc1/data1", "Test data 1")
    self._check_zk_output("ls -l /zk/test_nj/vt/zkocc1", """total: 3
  -rw-rw-rw- zk zk       11  %s data1
  -rw-rw-rw- zk zk       11  %s data2
  -rw-rw-rw- zk zk       11  %s data3
  """ % (_format_time(zk_data[0]['Stat']['MTime']),
         _format_time(zk_data[1]['Stat']['MTime']),
         _format_time(zk_data[2]['Stat']['MTime'])))

    # test /zk/local is not resolved and rejected
    out, err = utils.run(utils.vtroot+'/bin/zkclient2 -server localhost:%u /zk/local/vt/zkocc1/data1' % utils.zkocc_port_base, trap_output=True, raise_on_error=False)
    self.assertIn("zkocc: cannot resolve local cell", err)

    # start a background process to query the same value over and over again
    # while we kill the zk server and restart it
    outfd = tempfile.NamedTemporaryFile(dir=utils.tmp_root, delete=False)
    filename = outfd.name
    querier = utils.run_bg('/bin/bash -c "while true ; do '+utils.vtroot+'/bin/zkclient2 -server localhost:%u /zk/test_nj/vt/zkocc1/data1 ; sleep 0.1 ; done"' % utils.zkocc_port_base, stderr=outfd.file)
    outfd.close()
    time.sleep(1)

    # kill zk server, sleep a bit, restart zk server, sleep a bit
    utils.run(utils.vtroot+'/bin/zkctl -zk.cfg 1@'+utils.hostname+':%u:%u:%u shutdown' % (utils.zk_port_base, utils.zk_port_base+1, utils.zk_port_base+2))
    time.sleep(3)
    utils.run(utils.vtroot+'/bin/zkctl -zk.cfg 1@'+utils.hostname+':%u:%u:%u start' % (utils.zk_port_base, utils.zk_port_base+1, utils.zk_port_base+2))
    time.sleep(3)

    utils.kill_sub_process(querier)

    logging.debug("Checking %s", filename)
    fd = open(filename, "r")
    state = 0
    for line in fd:
      if line == "/zk/test_nj/vt/zkocc1/data1 = Test data 1 (NumChildren=0, Version=0, Cached=true, Stale=false)\n":
        stale = False
      elif line == "/zk/test_nj/vt/zkocc1/data1 = Test data 1 (NumChildren=0, Version=0, Cached=true, Stale=true)\n":
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
          self.fail('unexpected stale state')
    self.assertEqual(state, 2)
    fd.close()

    utils.zkocc_kill(zkocc_14850)

    # check that after the server is gone, the python client fails correctly
    level = logging.getLogger().getEffectiveLevel()
    logging.getLogger().setLevel(logging.ERROR)
    try:
      zkocc_client.get("/zk/test_nj/vt/zkocc1/data1")
      self.fail('exception expected')
    except zkocc.ZkOccError as e:
      if str(e) != "Cannot dial to any server":
        raise
    logging.getLogger().setLevel(level)

  def test_zkocc_qps(self):
    # preload the test_nj cell
    zkocc_14850 = utils.zkocc_start()

    qpser = utils.run_bg(utils.vtroot+'/bin/zkclient2 -server localhost:%u -mode qps /zk/test_nj/vt/zkocc1/data1 /zk/test_nj/vt/zkocc1/data2' % utils.zkocc_port_base)
    time.sleep(10)
    utils.kill_sub_process(qpser)

    # get the zkocc vars, make sure we have what we need
    v = utils.get_vars(utils.zkocc_port_base)
    if v['ZkReader']['test_nj']['State']['Current'] != 'Connected':
      raise utils.TestError('invalid zk global state: ', v['ZkReader']['test_nj']['State']['Current'])
    if v['ZkReader']['test_nj']['State']['DurationConnected'] < 9e9:
      self.fail('not enough time in Connected state: %s' %v['ZkReader']['test_nj']['State']['DurationConnected'])

    # some checks on performance / stats
    # a typical workstation will do 15k QPS, check we have more than 3k
    rpcCalls = v['ZkReader']['RpcCalls']
    if rpcCalls < 30000:
      self.fail('QPS is too low: %u < 30000' % rpcCalls / 10)
    cacheReads = v['ZkReader']['test_nj']['CacheReads']
    if cacheReads < 30000:
      self.fail('Cache QPS is too low: %u < 30000' % cacheReads / 10)
    totalCacheReads = v['ZkReader']['total']['CacheReads']
    self.assertEqual(cacheReads, totalCacheReads, 'Rollup stats are wrong')
    self.assertEqual(v['ZkReader']['UnknownCellErrors'], 0, 'unexpected UnknownCellErrors')
    utils.zkocc_kill(zkocc_14850)

  def test_fake_zkocc_connection(self):
    fkc = zkocc.FakeZkOccConnection.from_data_path("testing", "fake_zkocc_config.json")
    fkc.replace_zk_data("3306", "3310")
    fkc.replace_zk_data("127.0.0.1", "my.cool.hostname")

    # old style API tests
    keyspaces = fkc.children("/zk/testing/vt/ns")
    self.assertEqual(keyspaces['Children'], ["test_keyspace"], "children doesn't work")
    entry = fkc.get("/zk/testing/vt/ns/test_keyspace/0/master")
    self.assertEqual('{"entries": [{"host": "my.cool.hostname", "named_port_map": {"_mysql": 3310, "_vtocc": 6711}, "uid": 0, "port": 0}]}', entry['Data'], 'Entry fix-up is wrong')

    # new style API tests
    keyspaces = fkc.get_srv_keyspace_names('testing')
    self.assertEqual(keyspaces, ["test_keyspace"], "get_srv_keyspace_names doesn't work")
    keyspace = fkc.get_srv_keyspace('testing', 'test_keyspace')
    self.assertEqual({
        'Shards': [{
            'AddrsByType': None,
            'KeyRange': {'End': '', 'Start': ''},
            'ReadOnly': False}],
        'TabletTypes': ['rdonly', 'replica', 'master']},
                     keyspace, "keyspace reading is wrong")
    end_points = fkc.get_end_points("testing", "test_keyspace", "0", "master")
    self.assertEqual({
        'entries': [{'host': 'my.cool.hostname',
                     'named_port_map': {'_mysql': 3310, '_vtocc': 6711},
                     'port': 0,
                     'uid': 0}]},
                     end_points, "end points are wrong")

if __name__ == '__main__':
  utils.main()
