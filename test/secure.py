#!/usr/bin/python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import subprocess

from vtdb import tablet3
from vtdb import topology
from zk import zkocc

import os
import utils
import tablet

# single shard / 2 tablets
shard_0_master = tablet.Tablet()
shard_0_slave = tablet.Tablet()

cert_dir = utils.tmp_root + "/certs"

# FIXME(alainjobart) use the mysql certs for mysql replication tests
# I have trouble generating the right my.cnf, may add some env. variables
# to find the templates, and generate a different one for this test.

def openssl(cmd):
  result = subprocess.call(["openssl"] + cmd)
  if result != 0:
    raise utils.TestError("OpenSSL command failed: %s" % " ".join(cmd))

def setup():
  utils.zk_setup()

  utils.debug("Creating certificates")
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
  openssl(["req", "-newkey", "rsa:2048", "-days", "3600", "-nodes", "-batch",
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

def teardown():
  if utils.options.skip_teardown:
    return

  teardown_procs = [
      shard_0_master.teardown_mysql(),
      shard_0_slave.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  utils.zk_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_slave.remove_tree()

def run_test_secure():
  utils.run_vtctl('CreateKeyspace test_keyspace')

  shard_0_master.init_tablet('master',  'test_keyspace', '0')
  shard_0_slave.init_tablet('replica',  'test_keyspace', '0')

  utils.run_vtctl('RebuildShardGraph test_keyspace/0', auto_log=True)

  utils.run_vtctl('RebuildKeyspaceGraph test_keyspace', auto_log=True)

  zkocc_server = utils.zkocc_start()

  # create databases so vttablet can start behaving normally
  shard_0_master.create_db('vt_test_keyspace')
  shard_0_slave.create_db('vt_test_keyspace')

  # start the tablets
  shard_0_master.start_vttablet(cert=cert_dir + "/vt-server-cert.pem",
                                key=cert_dir + "/vt-server-key.pem")
  shard_0_slave.start_vttablet(cert=cert_dir + "/vt-server-cert.pem",
                               key=cert_dir + "/vt-server-key.pem",
                               repl_extra_flags={
      'flags': 2048,
      'ssl_ca': cert_dir + "/ca-cert.pem",
      'ssl_cert': cert_dir + "/client-cert.pem",
      'ssl_key': cert_dir + "/client-key.pem",
      })

  # Reparent using SSL
  utils.run_vtctl('ReparentShard -force test_keyspace/0 ' + shard_0_master.tablet_alias, auto_log=True)

  # then get the topology and check it
  zkocc_client = zkocc.ZkOccConnection("localhost:%u" % utils.zkocc_port_base,
                                       "test_nj", 30.0)
  topology.read_keyspaces(zkocc_client)

  shard_0_master_addrs = topology.get_host_port_by_name(zkocc_client, "test_keyspace.0.master:_vts")
  if len(shard_0_master_addrs) != 1:
    raise utils.TestError('topology.get_host_port_by_name failed for "test_keyspace.0.master:_vts", got: %s' % " ".join(["%s:%u(%s)" % (h, p, str(e)) for (h, p, e) in shard_0_master_addrs]))
  if shard_0_master_addrs[0][2] != True:
    raise utils.TestError('topology.get_host_port_by_name failed for "test_keyspace.0.master:_vts" is not encrypted')
  utils.debug("shard 0 master addrs: %s" % " ".join(["%s:%u(%s)" % (h, p, str(e)) for (h, p, e) in shard_0_master_addrs]))

  # make sure asking for optionally secure connections works too
  auto_addrs = topology.get_host_port_by_name(zkocc_client, "test_keyspace.0.master:_vtocc", encrypted=True)
  if auto_addrs != shard_0_master_addrs:
    raise utils.TestError('topology.get_host_port_by_name doesn\'t resolve encrypted addresses properly: %s != %s' % (str(shard_0_master_addrs), str(auto_addrs)))

  # try to connect with regular client
  try:
    conn = tablet3.TabletConnection("%s:%u" % (shard_0_master_addrs[0][0], shard_0_master_addrs[0][1]),
                                    "test_keyspace", "0", 10.0)
    conn.dial()
    raise utils.TestError("No exception raised to secure port")
  except tablet3.FatalError as e:
    if not e.args[0][0].startswith('Unexpected EOF in handshake to'):
      raise utils.TestError("Unexpected exception: %s" % str(e))

  # connect to encrypted port
  conn = tablet3.TabletConnection("%s:%u" % (shard_0_master_addrs[0][0], shard_0_master_addrs[0][1]),
                                  "test_keyspace", "0", 5.0, encrypted=True)
  conn.dial()
  (results, rowcount, lastrowid, fields) = conn._execute("select 1 from dual", {})
  if (len(results) != 1 or \
        results[0][0] != 1):
    print "conn._execute returned:", results
    raise utils.TestError('wrong conn._execute output')

  # trigger a time out on a secure connection, see what exception we get
  try:
    conn._execute("select sleep(100) from dual", {})
    raise utils.TestError("No timeout exception")
  except tablet3.TimeoutError as e:
    utils.debug("Got the right exception for SSL timeout: %s" % str(e))

  # kill everything
  utils.kill_sub_process(zkocc_server)
  shard_0_master.kill_vttablet()
  shard_0_slave.kill_vttablet()

def run_all():
  run_test_secure()

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
