#!/usr/bin/env python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import logging
import os
import unittest

import environment
import utils
import tablet

# single shard / 2 tablets
shard_0_master = tablet.Tablet()
shard_0_slave = tablet.Tablet()

cert_dir = environment.tmproot + '/certs'


def setUpModule():
  try:
    environment.topo_server().setup()

    logging.debug('Creating certificates')
    os.makedirs(cert_dir)

    utils.run(environment.binary_args('vttlstest') +
              ['-root', cert_dir,
               'CreateCA'])
    utils.run(environment.binary_args('vttlstest') +
              ['-root', cert_dir,
               'CreateSignedCert',
               '-common_name', 'Mysql Server',
               '-serial', '01',
               'server'])
    utils.run(environment.binary_args('vttlstest') +
              ['-root', cert_dir,
               'CreateSignedCert',
               '-common_name', 'Mysql Client',
               '-serial', '02',
               'client'])

    extra_my_cnf = cert_dir + '/secure.cnf'
    fd = open(extra_my_cnf, 'w')
    fd.write('ssl-ca=' + cert_dir + '/ca-cert.pem\n')
    fd.write('ssl-cert=' + cert_dir + '/server-cert.pem\n')
    fd.write('ssl-key=' + cert_dir + '/server-key.pem\n')
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
