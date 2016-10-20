#!/usr/bin/env python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import logging
import os
import subprocess
import unittest

import environment
import utils
import tablet

# single shard / 2 tablets
shard_0_master = tablet.Tablet()
shard_0_slave = tablet.Tablet()

cert_dir = environment.tmproot + '/certs'


def openssl(cmd):
  result = subprocess.call(['openssl'] + cmd, stderr=utils.devnull)
  if result != 0:
    raise utils.TestError('OpenSSL command failed: %s' % ' '.join(cmd))


def setUpModule():
  try:
    environment.topo_server().setup()

    logging.debug('Creating certificates')
    os.makedirs(cert_dir)

    # Create CA certificate
    ca_key = cert_dir + '/ca-key.pem'
    ca_cert = cert_dir + '/ca-cert.pem'
    openssl(['genrsa', '-out', cert_dir + '/ca-key.pem'])
    ca_config = cert_dir + '/ca.config'
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
    openssl(['req', '-new', '-x509', '-nodes', '-days', '3600', '-batch',
             '-config', ca_config,
             '-key', ca_key,
             '-out', ca_cert])

    # Create mysql server certificate, remove passphrase, and sign it
    server_key = cert_dir + '/server-key.pem'
    server_cert = cert_dir + '/server-cert.pem'
    server_req = cert_dir + '/server-req.pem'
    server_config = cert_dir + '/server.config'
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
    openssl(['req', '-newkey', 'rsa:2048', '-days', '3600', '-nodes', '-batch',
             '-config', server_config,
             '-keyout', server_key, '-out', server_req])
    openssl(['rsa', '-in', server_key, '-out', server_key])
    openssl(['x509', '-req',
             '-in', server_req,
             '-days', '3600',
             '-CA', ca_cert,
             '-CAkey', ca_key,
             '-set_serial', '01',
             '-out', server_cert])

    # Create mysql client certificate, remove passphrase, and sign it
    client_key = cert_dir + '/client-key.pem'
    client_cert = cert_dir + '/client-cert.pem'
    client_req = cert_dir + '/client-req.pem'
    client_config = cert_dir + '/client.config'
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
    openssl(['req', '-newkey', 'rsa:2048', '-days', '3600', '-nodes', '-batch',
             '-config', client_config,
             '-keyout', client_key, '-out', client_req])
    openssl(['rsa', '-in', client_key, '-out', client_key])
    openssl(['x509', '-req',
             '-in', client_req,
             '-days', '3600',
             '-CA', ca_cert,
             '-CAkey', ca_key,
             '-set_serial', '02',
             '-out', client_cert])

    extra_my_cnf = cert_dir + '/secure.cnf'
    fd = open(extra_my_cnf, 'w')
    fd.write('ssl-ca=' + ca_cert + '\n')
    fd.write('ssl-cert=' + server_cert + '\n')
    fd.write('ssl-key=' + server_key + '\n')
    fd.close()

    setup_procs = [
        shard_0_master.init_mysql(extra_my_cnf=extra_my_cnf),
        shard_0_slave.init_mysql(extra_my_cnf=extra_my_cnf),
        ]
    utils.wait_procs(setup_procs)

    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    shard_0_master.init_tablet('replica', 'test_keyspace', '0')
    shard_0_slave.init_tablet('replica', 'test_keyspace', '0')

    # create databases so vttablet can start behaving normally
    shard_0_master.create_db('vt_test_keyspace')
    shard_0_slave.create_db('vt_test_keyspace')
  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
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
  """This test makes sure that we can use SSL replication with Vitess.
  """

  def test_secure(self):
    # start the tablets
    shard_0_master.start_vttablet(wait_for_state='NOT_SERVING')
    shard_0_slave.start_vttablet(wait_for_state='NOT_SERVING',
                                 repl_extra_flags={
                                     'flags': '2048',
                                     'ssl-ca': cert_dir + '/ca-cert.pem',
                                     'ssl-cert': cert_dir + '/client-cert.pem',
                                     'ssl-key': cert_dir + '/client-key.pem',
                                 })

    # Reparent using SSL (this will also check replication works)
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     shard_0_master.tablet_alias], auto_log=True)

if __name__ == '__main__':
  utils.main()
