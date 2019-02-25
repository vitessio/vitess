#!/usr/bin/env python
#
# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This test makes sure encrypted transport over gRPC works.

The security chains are setup the following way:

* root CA
  * vttablet server CA
    * vttablet server instance cert/key
  * vttablet client CA
    * vttablet client 1 cert/key
  * vtgate server CA
    * vtgate server instance cert/key (common name is 'localhost')
  * vtgate client CA
    * vtgate client 1 cert/key
    * vtgate client 2 cert/key

The following table shows all the checks we perform:
process:            will check its peer is signed by:  for link:

  vttablet             vttablet client CA                vtgate -> vttablet
  vtgate               vttablet server CA                vtgate -> vttablet

  vtgate               vtgate client CA                  client -> vtgate
  client               vtgate server CA                  client -> vtgate

Additionally, we have the following constraints:
- the client certificate common name is used as immediate
caller ID by vtgate, and forwarded to vttablet. This allows us to use
table ACLs on the vttablet side.
- the vtgate server certificate common name is set to 'localhost' so it matches
the hostname dialed by the vtgate clients. This is not a requirement for the
go client, that can set its expected server name. However, the python gRPC
client doesn't have the ability to set the server name, so they must match.
- the python client needs to have the full chain for the server validation
(that is 'vtgate server CA' + 'root CA'). A go client doesn't. So we read both
below when using the python client, but we only pass the intermediate cert
to the go clients (for vtgate -> vttablet link).
"""

import logging
import os
import unittest

from vtdb import vtgate_client
from vtdb import dbexceptions

import environment
import utils
import tablet

# single shard / 2 tablets
shard_0_master = tablet.Tablet()
shard_0_slave = tablet.Tablet()

cert_dir = environment.tmproot + '/certs'
table_acl_config = environment.tmproot + '/table_acl_config.json'


def create_signed_cert(ca, serial, name, common_name):
  logging.info('Creating signed cert and key %s', common_name)
  utils.run(environment.binary_args('vttlstest') +
            ['-root', cert_dir,
             'CreateSignedCert',
             '-parent', ca,
             '-serial', serial,
             '-common_name', common_name,
             name])


def server_extra_args(name, ca):
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


def python_client_kwargs(name, ca):
  with open(cert_dir + '/ca-cert.pem', 'r') as fd:
    root_ca_contents = fd.read()
  with open(cert_dir + '/' + ca + '-cert.pem', 'r') as fd:
    ca_contents = fd.read()
  with open(cert_dir + '/' + name + '-key.pem', 'r') as fd:
    key_contents = fd.read()
  with open(cert_dir + '/' + name + '-cert.pem', 'r') as fd:
    cert_contents = fd.read()
  return {
      'root_certificates': root_ca_contents+ca_contents,
      'private_key': key_contents,
      'certificate_chain': cert_contents,
  }


def setUpModule():
  try:
    environment.topo_server().setup()

    logging.debug('Creating certificates')
    os.makedirs(cert_dir)

    # Create CA certificate
    utils.run(environment.binary_args('vttlstest') +
              ['-root', cert_dir,
               'CreateCA'])

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
                       'localhost')

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
    with open(table_acl_config, 'w') as fd:
      fd.write("""{
      "table_groups": [
          {
             "table_names_or_prefixes": ["vt_insert_test"],
             "readers": ["vtgate client 1"],
             "writers": ["vtgate client 1"],
             "admins": ["vtgate client 1"]
          }
      ]
}
""")

    # start the tablets
    shard_0_master.start_vttablet(
        wait_for_state='NOT_SERVING',
        table_acl_config=table_acl_config,
        extra_args=server_extra_args('vttablet-server-instance',
                                     'vttablet-client'))
    shard_0_slave.start_vttablet(
        wait_for_state='NOT_SERVING',
        table_acl_config=table_acl_config,
        extra_args=server_extra_args('vttablet-server-instance',
                                     'vttablet-client'))

    # setup replication
    utils.run_vtctl(tmclient_extra_args('vttablet-client-1') + [
        'InitShardMaster', '-force', 'test_keyspace/0',
        shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(tmclient_extra_args('vttablet-client-1') + [
        'ApplySchema', '-sql', create_vt_insert_test,
        'test_keyspace'])
    for t in [shard_0_master, shard_0_slave]:
      utils.run_vtctl(tmclient_extra_args('vttablet-client-1') + [
          'RunHealthCheck', t.tablet_alias])

    # start vtgate
    utils.VtGate().start(extra_args=tabletconn_extra_args('vttablet-client-1')+
                         server_extra_args('vtgate-server-instance',
                                           'vtgate-client'))

    # 'vtgate client 1' is authorized to access vt_insert_test
    protocol, addr = utils.vtgate.rpc_endpoint(python=True)
    conn = vtgate_client.connect(protocol, addr, 30.0,
                                 **python_client_kwargs('vtgate-client-1',
                                                        'vtgate-server'))
    cursor = conn.cursor(tablet_type='master', keyspace='test_keyspace',
                         shards=['0'])
    cursor.execute('select * from vt_insert_test', {})
    conn.close()

    # 'vtgate client 2' is not authorized to access vt_insert_test
    conn = vtgate_client.connect(protocol, addr, 30.0,
                                 **python_client_kwargs('vtgate-client-2',
                                                        'vtgate-server'))
    try:
      cursor = conn.cursor(tablet_type='master', keyspace='test_keyspace',
                           shards=['0'])
      cursor.execute('select * from vt_insert_test', {})
      self.fail('Execute went through')
    except dbexceptions.DatabaseError, e:
      s = str(e)
      self.assertIn('table acl error', s)
      self.assertIn('cannot run PASS_SELECT on table', s)
    conn.close()

    # now restart vtgate in the mode where we don't use SSL
    # for client connections, but we copy effective caller id
    # into immediate caller id.
    utils.vtgate.kill()
    utils.VtGate().start(extra_args=tabletconn_extra_args('vttablet-client-1')+
                         ['-grpc_use_effective_callerid'])

    protocol, addr = utils.vtgate.rpc_endpoint(python=True)
    conn = vtgate_client.connect(protocol, addr, 30.0)
    cursor = conn.cursor(tablet_type='master', keyspace='test_keyspace',
                         shards=['0'])

    # not passing any immediate caller id should fail as using
    # the unsecure user "unsecure_grpc_client"
    cursor.set_effective_caller_id(None)
    try:
      cursor.execute('select * from vt_insert_test', {})
      self.fail('Execute went through')
    except dbexceptions.DatabaseError, e:
      s = str(e)
      self.assertIn('table acl error', s)
      self.assertIn('cannot run PASS_SELECT on table', s)
      self.assertIn('unsecure_grpc_client', s)

    # 'vtgate client 1' is authorized to access vt_insert_test
    cursor.set_effective_caller_id(vtgate_client.CallerID(
        principal='vtgate client 1'))
    cursor.execute('select * from vt_insert_test', {})

    # 'vtgate client 2' is not authorized to access vt_insert_test
    cursor.set_effective_caller_id(vtgate_client.CallerID(
        principal='vtgate client 2'))
    try:
      cursor.execute('select * from vt_insert_test', {})
      self.fail('Execute went through')
    except dbexceptions.DatabaseError, e:
      s = str(e)
      self.assertIn('table acl error', s)
      self.assertIn('cannot run PASS_SELECT on table', s)

    conn.close()


if __name__ == '__main__':
  utils.main()
