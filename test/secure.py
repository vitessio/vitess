#!/usr/bin/env python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import logging
import os
import subprocess
import time
import unittest

from vtdb import dbexceptions
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import tablet as tablet3
from vtdb import topology
from vtdb import vtgatev2
from zk import zkocc

import environment
import utils
import tablet

# single shard / 2 tablets
shard_0_master = tablet.Tablet()
shard_0_slave = tablet.Tablet()

cert_dir = environment.tmproot + "/certs"

def openssl(cmd):
  result = subprocess.call(["openssl"] + cmd, stderr=utils.devnull)
  if result != 0:
    raise utils.TestError("OpenSSL command failed: %s" % " ".join(cmd))

def setUpModule():
  try:
    environment.topo_server().setup()

    logging.debug("Creating certificates")
    os.makedirs(cert_dir)

    # Create CA certificate
    ca_key = cert_dir + "/ca-key.pem"
    ca_cert = cert_dir + "/ca-cert.pem"
    openssl(["genrsa", "-out", cert_dir + "/ca-key.pem"])
    ca_config = cert_dir + "/ca.config"
    with open(ca_config, 'w') as fd:
      fd.write("""
[ req ]
 default_bits           = 1024
 default_keyfile        = keyfile.pem
 distinguished_name     = req_distinguished_name
 attributes             = req_attributes
 prompt                 = no
 output_password        = mypass
[ req_distinguished_name ]
 C                      = US
 ST                     = California
 L                      = Mountain View
 O                      = Google
 OU                     = Vitess
 CN                     = Mysql CA
 emailAddress           = test@email.address
[ req_attributes ]
 challengePassword      = A challenge password
""")
    openssl(["req", "-new", "-x509", "-nodes", "-days", "3600", "-batch",
             "-config", ca_config,
             "-key", ca_key,
             "-out", ca_cert])

    # Create mysql server certificate, remove passphrase, and sign it
    server_key = cert_dir + "/server-key.pem"
    server_cert = cert_dir + "/server-cert.pem"
    server_req = cert_dir + "/server-req.pem"
    server_config = cert_dir + "/server.config"
    with open(server_config, 'w') as fd:
      fd.write("""
[ req ]
 default_bits           = 1024
 default_keyfile        = keyfile.pem
 distinguished_name     = req_distinguished_name
 attributes             = req_attributes
 prompt                 = no
 output_password        = mypass
[ req_distinguished_name ]
 C                      = US
 ST                     = California
 L                      = Mountain View
 O                      = Google
 OU                     = Vitess
 CN                     = Mysql Server
 emailAddress           = test@email.address
[ req_attributes ]
 challengePassword      = A challenge password
""")
    openssl(["req", "-newkey", "rsa:2048", "-days", "3600", "-nodes", "-batch",
             "-config", server_config,
             "-keyout", server_key, "-out", server_req])
    openssl(["rsa", "-in", server_key, "-out", server_key])
    openssl(["x509", "-req",
             "-in", server_req,
             "-days", "3600",
             "-CA", ca_cert,
             "-CAkey", ca_key,
             "-set_serial", "01",
             "-out", server_cert])

    # Create mysql client certificate, remove passphrase, and sign it
    client_key = cert_dir + "/client-key.pem"
    client_cert = cert_dir + "/client-cert.pem"
    client_req = cert_dir + "/client-req.pem"
    client_config = cert_dir + "/client.config"
    with open(client_config, 'w') as fd:
      fd.write("""
[ req ]
 default_bits           = 1024
 default_keyfile        = keyfile.pem
 distinguished_name     = req_distinguished_name
 attributes             = req_attributes
 prompt                 = no
 output_password        = mypass
[ req_distinguished_name ]
 C                      = US
 ST                     = California
 L                      = Mountain View
 O                      = Google
 OU                     = Vitess
 CN                     = Mysql Client
 emailAddress           = test@email.address
[ req_attributes ]
 challengePassword      = A challenge password
""")
    openssl(["req", "-newkey", "rsa:2048", "-days", "3600", "-nodes", "-batch",
             "-config", client_config,
             "-keyout", client_key, "-out", client_req])
    openssl(["rsa", "-in", client_key, "-out", client_key])
    openssl(["x509", "-req",
             "-in", client_req,
             "-days", "3600",
             "-CA", ca_cert,
             "-CAkey", ca_key,
             "-set_serial", "02",
             "-out", client_cert])

    # Create vt server certificate, remove passphrase, and sign it
    vt_server_key = cert_dir + "/vt-server-key.pem"
    vt_server_cert = cert_dir + "/vt-server-cert.pem"
    vt_server_req = cert_dir + "/vt-server-req.pem"
    vt_server_config = cert_dir + "/server.config"
    with open(vt_server_config, 'w') as fd:
      fd.write("""
[ req ]
 default_bits           = 1024
 default_keyfile        = keyfile.pem
 distinguished_name     = req_distinguished_name
 attributes             = req_attributes
 prompt                 = no
 output_password        = mypass
[ req_distinguished_name ]
 C                      = US
 ST                     = California
 L                      = Mountain View
 O                      = Google
 OU                     = Vitess
 CN                     = Vitess Server
 emailAddress           = test@email.address
[ req_attributes ]
 challengePassword      = A challenge password
""")
    openssl(["req", "-newkey", "rsa:2048", "-days", "3600", "-nodes", "-batch",
             "-config", vt_server_config,
             "-keyout", vt_server_key, "-out", vt_server_req])
    openssl(["rsa", "-in", vt_server_key, "-out", vt_server_key])
    openssl(["x509", "-req",
             "-in", vt_server_req,
             "-days", "3600",
             "-CA", ca_cert,
             "-CAkey", ca_key,
             "-set_serial", "03",
             "-out", vt_server_cert])

    extra_my_cnf = cert_dir + "/secure.cnf"
    fd = open(extra_my_cnf, "w")
    fd.write("ssl-ca=" + ca_cert + "\n")
    fd.write("ssl-cert=" + server_cert + "\n")
    fd.write("ssl-key=" + server_key + "\n")
    fd.close()

    setup_procs = [
        shard_0_master.init_mysql(extra_my_cnf=extra_my_cnf),
        shard_0_slave.init_mysql(extra_my_cnf=extra_my_cnf),
        ]
    utils.wait_procs(setup_procs)

    utils.run_vtctl('CreateKeyspace test_keyspace')

    shard_0_master.init_tablet('master',  'test_keyspace', '0')
    shard_0_slave.init_tablet('replica',  'test_keyspace', '0')

    utils.run_vtctl('RebuildKeyspaceGraph test_keyspace', auto_log=True)

    # create databases so vttablet can start behaving normally
    shard_0_master.create_db('vt_test_keyspace')
    shard_0_slave.create_db('vt_test_keyspace')
  except:
    tearDownModule()
    raise

def tearDownModule():
  if utils.options.skip_teardown:
    return

  shard_0_master.kill_vttablet()
  shard_0_slave.kill_vttablet()

  teardown_procs = [
      shard_0_master.teardown_mysql(),
      shard_0_slave.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_slave.remove_tree()

class TestSecure(unittest.TestCase):

  def test_secure(self):
    vtgate_server, vtgate_port = utils.vtgate_start(cache_ttl='0s')

    # start the tablets
    shard_0_master.start_vttablet(cert=cert_dir + "/vt-server-cert.pem",
                                  key=cert_dir + "/vt-server-key.pem")
    shard_0_slave.start_vttablet(cert=cert_dir + "/vt-server-cert.pem",
                                 key=cert_dir + "/vt-server-key.pem",
                                 repl_extra_flags={
        'flags': "2048",
        'ssl-ca': cert_dir + "/ca-cert.pem",
        'ssl-cert': cert_dir + "/client-cert.pem",
        'ssl-key': cert_dir + "/client-key.pem",
        })

    # Reparent using SSL
    for t in [shard_0_master, shard_0_slave]:
      t.reset_replication()
    utils.run_vtctl('ReparentShard -force test_keyspace/0 ' + shard_0_master.tablet_alias, auto_log=True)

    # then get the topology and check it
    topo_client = zkocc.ZkOccConnection("localhost:%u" % vtgate_port,
                                        "test_nj", 30.0)
    topology.read_keyspaces(topo_client)

    shard_0_master_addrs = topology.get_host_port_by_name(topo_client, "test_keyspace.0.master:vts")
    if len(shard_0_master_addrs) != 1:
      self.fail('topology.get_host_port_by_name failed for "test_keyspace.0.master:vts", got: %s' % " ".join(["%s:%u(%s)" % (h, p, str(e)) for (h, p, e) in shard_0_master_addrs]))
    if shard_0_master_addrs[0][2] != True:
      self.fail('topology.get_host_port_by_name failed for "test_keyspace.0.master:vts" is not encrypted')
    logging.debug("shard 0 master addrs: %s", " ".join(["%s:%u(%s)" % (h, p, str(e)) for (h, p, e) in shard_0_master_addrs]))

    # make sure asking for optionally secure connections works too
    auto_addrs = topology.get_host_port_by_name(topo_client, "test_keyspace.0.master:vt", encrypted=True)
    if auto_addrs != shard_0_master_addrs:
      self.fail('topology.get_host_port_by_name doesn\'t resolve encrypted addresses properly: %s != %s' % (str(shard_0_master_addrs), str(auto_addrs)))

    # try to connect with regular client
    try:
      conn = tablet3.TabletConnection("%s:%u" % (shard_0_master_addrs[0][0], shard_0_master_addrs[0][1]),
                                      "", "test_keyspace", "0", 10.0)
      conn.dial()
      self.fail("No exception raised to secure port")
    except dbexceptions.FatalError as e:
      if not e.args[0][0].startswith('Unexpected EOF in handshake to'):
        self.fail("Unexpected exception: %s" % str(e))

    sconn = utils.get_vars(shard_0_master.port)["SecureConnections"]
    if sconn != 0:
      self.fail("unexpected conns %s" % sconn)

    # connect to encrypted port
    conn = tablet3.TabletConnection("%s:%u" % (shard_0_master_addrs[0][0], shard_0_master_addrs[0][1]),
                                    "", "test_keyspace", "0", 5.0, encrypted=True)
    conn.dial()
    (results, rowcount, lastrowid, fields) = conn._execute("select 1 from dual", {})
    self.assertEqual(results, [(1,),], 'wrong conn._execute output: %s' % str(results))

    sconn = utils.get_vars(shard_0_master.port)["SecureConnections"]
    if sconn != 1:
      self.fail("unexpected conns %s" % sconn)
    saccept = utils.get_vars(shard_0_master.port)["SecureAccepts"]
    if saccept == 0:
      self.fail("unexpected accepts %s" % saccept)

    # trigger a time out on a secure connection, see what exception we get
    try:
      conn._execute("select sleep(100) from dual", {})
      self.fail("No timeout exception")
    except dbexceptions.TimeoutError as e:
      logging.debug("Got the right exception for SSL timeout: %s", str(e))

    # start a vtgate to connect to that tablet
    gate_proc, gate_port, gate_secure_port = utils.vtgate_start(
        tablet_bson_encrypted=True,
        cert=cert_dir + "/vt-server-cert.pem",
        key=cert_dir + "/vt-server-key.pem")

    # try to connect to vtgate with regular client
    timeout = 2.0
    try:
      conn = vtgatev2.connect(["localhost:%s" % (gate_secure_port),],
                               timeout)
      self.fail("No exception raised to VTGate secure port")
    except dbexceptions.OperationalError as e:
      exception_type = e.args[2]
      exception_msg = str(e.args[2][0][0])
      self.assertIsInstance(exception_type, dbexceptions.FatalError,
                            "unexpected exception type")
      if not exception_msg.startswith('Unexpected EOF in handshake to'):
        self.fail("Unexpected exception message: %s" % exception_msg)

    sconn = utils.get_vars(gate_port)["SecureConnections"]
    if sconn != 0:
      self.fail("unexpected conns %s" % sconn)

    # connect to vtgate with encrypted port
    conn = vtgatev2.connect(["localhost:%s" % (gate_secure_port),],
                             timeout, encrypted=True)
    (results, rowcount, lastrowid, fields) = conn._execute(
        "select 1 from dual",
        {},
        "test_keyspace",
        "master",
        keyranges=[keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE),])
    self.assertEqual(rowcount, 1, "want 1, got %d" % (rowcount))
    self.assertEqual(len(fields), 1, "want 1, got %d" % (len(fields)))
    self.assertEqual(results, [(1,),], 'wrong conn._execute output: %s' % str(results))

    sconn = utils.get_vars(gate_port)["SecureConnections"]
    if sconn != 1:
      self.fail("unexpected conns %s" % sconn)
    saccept = utils.get_vars(gate_port)["SecureAccepts"]
    if saccept == 0:
      self.fail("unexpected accepts %s" % saccept)

    # trigger a time out on a vtgate secure connection, see what exception we get
    try:
      conn._execute("select sleep(4) from dual",
                    {},
                    "test_keyspace",
                    "master",
                    keyranges=[keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE),])
      self.fail("No timeout exception")
    except dbexceptions.TimeoutError as e:
      logging.debug("Got the right exception for SSL timeout: %s", str(e))
    conn.close()
    utils.vtgate_kill(gate_proc)

    # kill everything
    utils.vtgate_kill(vtgate_server)

  def test_restart(self):
    shard_0_master.create_db('vt_test_keyspace')

    proc1 = shard_0_master.start_vttablet(cert=cert_dir + "/vt-server-cert.pem",
                                          key=cert_dir + "/vt-server-key.pem")
    # Takes a bit longer for vttablet to serve the pid port
    time.sleep(1.0)
    proc2 = shard_0_master.start_vttablet(cert=cert_dir + "/vt-server-cert.pem",
                                          key=cert_dir + "/vt-server-key.pem")
    time.sleep(1.0)
    proc1.poll()
    if proc1.returncode is None:
      self.fail("proc1 still running")
    shard_0_master.kill_vttablet()
    logging.debug("Done here")

if __name__ == '__main__':
  utils.main()
