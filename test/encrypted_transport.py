#!/usr/bin/env python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""This test makes sure encrypted transport over gRPC works.

The security chains are setup the following way:

* root CA
  * vttablet server CA
    * vttablet server instance cert/key
  * vttablet client CA
    * vttablet client 1 cert/key
  * vtgate server CA
    * vtgate server instance cert/key
  * vtgate client CA
    * vtgate client 1 cert/key
    * vtgate client 2 cert/key

The following table shows all the checks we perform:
process:            will check its peer is signed by:  for link:

  vttablet             vttablet client CA                vtgate -> vttablet
  vtgate               vttablet server CA                vtgate -> vttablet

  vtgate               vtgate client CA                  client -> vtgate
  client               vtgate server CA                  client -> vtgate

Additionnally, the client certificate common name is used as immediate
caller ID by vtgate, and forwarded to vttablet. This allows us to use
table ACLs on the vttablet side.

"""

import logging
import os
import subprocess
import unittest

from vtdb import vtgate_client

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


def create_signed_cert(ca, serial, name, common_name):
  logging.info('Creating signed cert and key %s', common_name)
  ca_key = cert_dir + '/' + ca + '-key.pem'
  ca_cert = cert_dir + '/' + ca + '-cert.pem'
  key = cert_dir + '/' + name + '-key.pem'
  cert = cert_dir + '/' + name + '-cert.pem'
  req = cert_dir + '/' + name + '-req.pem'
  config = cert_dir + '/' + name + '.config'
  with open(config, 'w') as fd:
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
 CN                     = %s
 emailAddress           = test@email.address
[ req_attributes ]
 challengePassword      = A challenge password
""" % common_name)
  openssl(['req', '-newkey', 'rsa:2048', '-days', '3600', '-nodes', '-batch',
           '-config', config,
           '-keyout', key, '-out', req])
  openssl(['rsa', '-in', key, '-out', key])
  openssl(['x509', '-req',
           '-in', req,
           '-days', '3600',
           '-CA', ca_cert,
           '-CAkey', ca_key,
           '-set_serial', serial,
           '-out', cert])


def vttablet_extra_args(name):
  ca = 'vttablet-client'
  return [
      '-grpc_cert', cert_dir + '/' + name + '-cert.pem',
      '-grpc_key', cert_dir + '/' + name + '-key.pem',
      '-grpc_ca', cert_dir + '/' + ca + '-cert.pem',
  ]


def tmclient_extra_args(name):
  ca = 'vttablet-server'
  return [
      '-tablet_manager_grpc_cert', cert_dir + '/' + name + '-cert.pem',
      '-tablet_manager_grpc_key', cert_dir + '/' + name + '-key.pem',
      '-tablet_manager_grpc_ca', cert_dir + '/' + ca + '-cert.pem',
      '-tablet_manager_grpc_server_name', 'vttablet server instance',
  ]


def tabletconn_extra_args(name):
  ca = 'vttablet-server'
  return [
      '-tablet_grpc_cert', cert_dir + '/' + name + '-cert.pem',
      '-tablet_grpc_key', cert_dir + '/' + name + '-key.pem',
      '-tablet_grpc_ca', cert_dir + '/' + ca + '-cert.pem',
      '-tablet_grpc_server_name', 'vttablet server instance',
  ]


def setUpModule():
  try:
    environment.topo_server().setup()

    logging.debug('Creating certificates')
    os.makedirs(cert_dir)

    # Create CA certificate
    logging.info('Creating root CA')
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
 CN                     = CA
 emailAddress           = test@email.address
[ req_attributes ]
 challengePassword      = A challenge password
""")
    openssl(['req', '-new', '-x509', '-nodes', '-days', '3600', '-batch',
             '-config', ca_config,
             '-key', ca_key,
             '-out', ca_cert])

    # create all certs
    create_signed_cert('ca', '01', 'vttablet-server', 'vttablet server CA')
    create_signed_cert('ca', '02', 'vttablet-client', 'vttablet client CA')
    create_signed_cert('ca', '03', 'vtgate-server', 'vtgate server CA')
    create_signed_cert('ca', '04', 'vtgate-client', 'vtgate client CA')

    create_signed_cert('vttablet-server', '01', 'vttablet-server-instance',
                       'vttablet server instance')

    create_signed_cert('vttablet-client', '01', 'vttablet-client-1',
                       'vttablet client 1')

    create_signed_cert('vtgate-server', '01', 'vtgate-server-instance',
                       'vtgate server instance')

    create_signed_cert('vtgate-client', '01', 'vtgate-client-1',
                       'vtgate client 1')
    create_signed_cert('vtgate-client', '02', 'vtgate-client-2',
                       'vtgate client 2')

    # setup all processes
    setup_procs = [
        shard_0_master.init_mysql(),
        shard_0_slave.init_mysql(),
        ]
    utils.wait_procs(setup_procs)

    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    shard_0_master.init_tablet('master', 'test_keyspace', '0')
    shard_0_slave.init_tablet('replica', 'test_keyspace', '0')

    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

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


create_vt_insert_test = '''create table vt_insert_test (
id bigint auto_increment,
msg varchar(64),
keyspace_id bigint(20) unsigned NOT NULL,
primary key (id)
) Engine=InnoDB'''


class TestSecure(unittest.TestCase):
  """This test makes sure that we can use full TLS security within Vitess.
  """

  def test_secure(self):
    # start the tablets
    shard_0_master.start_vttablet(
        extra_args=vttablet_extra_args('vttablet-server-instance'))
    shard_0_slave.start_vttablet(
        extra_args=vttablet_extra_args('vttablet-server-instance'))

    # setup replication
    for t in [shard_0_master, shard_0_slave]:
      t.reset_replication()
    utils.run_vtctl(tmclient_extra_args('vttablet-client-1') + [
        'InitShardMaster', 'test_keyspace/0',
        shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(tmclient_extra_args('vttablet-client-1') + [
        'ApplySchema', '-sql', create_vt_insert_test,
        'test_keyspace'])
    for t in [shard_0_master, shard_0_slave]:
      utils.run_vtctl(tmclient_extra_args('vttablet-client-1') + [
          'RunHealthCheck', t.tablet_alias, 'replica'])

    # start vtgate
    utils.VtGate().start(extra_args=tabletconn_extra_args('vttablet-client-1'))

    protocol, addr = utils.vtgate.rpc_endpoint(python=True)
    conn = vtgate_client.connect(protocol, addr, 30.0)
    try:
      cursor = conn.cursor(tablet_type='master', keyspace='test_keyspace',
                           shards=['0'])
      cursor.execute('select * from vt_insert_test', {})
    except Exception, e:  # pylint: disable=broad-except
      self.fail('Execute failed w/ exception %s' % str(e))
    conn.close()


if __name__ == '__main__':
  utils.main()
