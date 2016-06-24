#!/usr/bin/env python
"""A vtctl webdriver test."""

import logging
import os
from selenium import webdriver
import unittest

import environment
import tablet
import utils


# range '' - 80
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
# range 80 - ''
shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()
# unsharded
shard_0_master_ks2 = tablet.Tablet()
shard_0_replica_ks2 = tablet.Tablet()
shard_0_rdonly_ks2 = tablet.Tablet()
# all tablets
tablets = [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica,
           shard_0_master_ks2, shard_0_replica_ks2, shard_0_rdonly_ks2]


def setUpModule():
  try:
    environment.topo_server().setup()

    setup_procs = [t.init_mysql() for t in tablets]
    utils.Vtctld().start()
    utils.wait_procs(setup_procs)

  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return

  teardown_procs = [t.teardown_mysql() for t in tablets]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  for t in tablets:
    t.remove_tree()


class TestVtctldWeb(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    """Set up two keyspaces: one unsharded, one with two shards."""
    # TODO(thompsonja): Add more cells
    utils.run_vtctl(['CreateKeyspace',
                     '--sharding_column_name', 'keyspace_id',
                     '--sharding_column_type', 'uint64',
                     'test_keyspace'])
    utils.run_vtctl(['CreateKeyspace',
                     '--sharding_column_name', 'keyspace_id',
                     '--sharding_column_type', 'uint64',
                     'test_keyspace2'])

    shard_0_master.init_tablet('master', 'test_keyspace', '-80')
    shard_0_replica.init_tablet('replica', 'test_keyspace', '-80')
    shard_1_master.init_tablet('master', 'test_keyspace', '80-')
    shard_1_replica.init_tablet('replica', 'test_keyspace', '80-')
    shard_0_master_ks2.init_tablet('master', 'test_keyspace2', '0')
    shard_0_replica_ks2.init_tablet('replica', 'test_keyspace2', '0')
    shard_0_rdonly_ks2.init_tablet('rdonly', 'test_keyspace2', '0')

    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace2'], auto_log=True)

    # start running all the tablets
    for t in tablets:
      t.create_db('vt_%s' % t.keyspace)
      t.start_vttablet(wait_for_state=None,
                       extra_args=utils.vtctld.process_args())

    # wait for the right states
    for t in tablets:
      t.wait_for_vttablet_state(
          'SERVING' if t.tablet_type == 'master' else 'NOT_SERVING')

    utils.run_vtctl(['InitShardMaster', 'test_keyspace/-80',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', 'test_keyspace/80-',
                     shard_1_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', 'test_keyspace2/0',
                     shard_0_master_ks2.tablet_alias], auto_log=True)
    shard_0_replica.wait_for_vttablet_state('SERVING')
    shard_0_replica_ks2.wait_for_vttablet_state('SERVING')

    # run checks now
    utils.validate_topology()

    os.environ['webdriver.chrome.driver'] = os.path.join(
        os.environ['VTROOT'], 'dist')

    # Only testing against Chrome for now
    cls.driver = webdriver.Chrome()

    cls.vtctl_addr = 'http://localhost:%d' % utils.vtctld.port

  @classmethod
  def tearDownClass(cls):
    cls.driver.quit()

  def _get_keyspaces(self):
    """Get list of all present keyspaces."""
    content = self.driver.find_element_by_id('content')
    # TODO(thompsonja) find better way to get keyspace name
    keyspaces = content.find_elements_by_tag_name('md-card')
    return [ks.find_element_by_tag_name('h2').text for ks in keyspaces]

  def _get_keyspace_element(self, keyspace_name):
    """Get a specific keyspace element given a keyspace name."""
    return self.driver.find_element_by_id('%s-card' % keyspace_name)

  def _get_shards(self, keyspace_name):
    shard_grid = self.driver.find_element_by_id('%s-shard-list' % keyspace_name)
    return shard_grid.text.split('\n')

  def _get_shard_element(self, keyspace_name, shard_name):
    return self._get_keyspace_element(keyspace_name).find_element_by_link_text(
        shard_name)

  def _get_tablet_names(self):
    tablet_elements = (
        self.driver.find_element_by_id('tablets').find_elements_by_tag_name(
            'md-card'))
    tablet_titles = [
        x.find_element_by_tag_name('md-toolbar').text.split('\n')[0]
        for x in tablet_elements]
    return dict(
        [(x.split(' ')[0], x.split(' ')[1][1:-1]) for x in tablet_titles])

  def _get_shard_record_keyspace_shard(self):
    return self.driver.find_element_by_id('keyspace-shard').text

  def _get_shard_record_master_tablet(self):
    return self.driver.find_element_by_id('master-tablet').text

  def _check_tablet_types(self, tablet_types, expected_counts):
    for expected_type, count in expected_counts.iteritems():
      self.assertEquals(count,
                        len([x for x in tablet_types if x == expected_type]))

  def _check_shard_overview(
      self, keyspace_name, shard_name, expected_tablet_types):
    logging.info('Checking %s/%s', keyspace_name, shard_name)
    self._get_shard_element(keyspace_name, shard_name).click()
    self.assertEquals(self._get_shard_record_keyspace_shard(),
                      '%s/%s' % (keyspace_name, shard_name))
    master = self._get_shard_record_master_tablet()
    logging.info('master tablet is %s', master)
    shard_tablets = self._get_tablet_names()
    self.assertEquals(shard_tablets[master], 'master')
    self._check_tablet_types(shard_tablets.values(), expected_tablet_types)
    self.driver.back()

  def test_keyspace_overview(self):
    logging.info('Testing keyspace overview')

    logging.info('Fetching main vtctl page: %s', self.vtctl_addr)
    self.driver.get(self.vtctl_addr)

    keyspace_names = self._get_keyspaces()
    logging.info('Keyspaces: %s', ', '.join(keyspace_names))
    self.assertListEqual(['test_keyspace', 'test_keyspace2'], keyspace_names)

    test_keyspace_shards = self._get_shards('test_keyspace')
    logging.info(
        'Shards in test_keyspace: %s', ', '.join(test_keyspace_shards))
    self.assertListEqual(test_keyspace_shards, ['-80', '80-'])

    test_keyspace2_shards = self._get_shards('test_keyspace2')
    logging.info(
        'Shards in test_keyspace2: %s', ', '.join(test_keyspace2_shards))
    self.assertListEqual(test_keyspace2_shards, ['0'])

  def test_shard_overview(self):
    logging.info('Testing shard overview')

    logging.info('Fetching main vtctl page: %s', self.vtctl_addr)
    self.driver.get(self.vtctl_addr)

    self._check_shard_overview(
        'test_keyspace', '-80', {'master': 1, 'replica': 1, 'rdonly': 0})
    self._check_shard_overview(
        'test_keyspace', '80-', {'master': 1, 'replica': 1, 'rdonly': 0})
    self._check_shard_overview(
        'test_keyspace2', '0', {'master': 1, 'replica': 1, 'rdonly': 1})


if __name__ == '__main__':
  utils.main()
