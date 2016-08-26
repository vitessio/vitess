#!/usr/bin/env python
"""A vtctl webdriver test."""

import logging
import os
from selenium import webdriver
import unittest

from vtproto import vttest_pb2
from vttest import environment as vttest_environment
from vttest import local_database
from vttest import mysql_flavor

import environment
import utils
from selenium.common.exceptions import NoSuchElementException


def setUpModule():
  try:
    if utils.options.xvfb:
      try:
        # This will be killed automatically by utils.kill_sub_processes()
        utils.run_bg(['Xvfb', ':15', '-ac'])
        os.environ['DISPLAY'] = ':15'
      except OSError as err:
        # Despite running in background, utils.run_bg() will throw immediately
        # if the Xvfb binary is not found.
        logging.error(
            "Can't start Xvfb (will try local DISPLAY instead): %s", err)
  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return
  utils.remove_tmp_files()


class TestVtctldWeb(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    """Set up two keyspaces: one unsharded, one with two shards."""
    topology = vttest_pb2.VTTestTopology()
    topology.cells.append('test')
    keyspace = topology.keyspaces.add(name='test_keyspace')
    keyspace.replica_count = 2
    keyspace.rdonly_count = 2
    keyspace.shards.add(name='-80')
    keyspace.shards.add(name='80-')
    keyspace2 = topology.keyspaces.add(name='test_keyspace2')
    keyspace2.shards.add(name='0')
    keyspace2.replica_count = 2
    keyspace2.rdonly_count = 1

    if os.environ.get('CI') == 'true' and os.environ.get('TRAVIS') == 'true':
      username = os.environ['SAUCE_USERNAME']
      access_key = os.environ['SAUCE_ACCESS_KEY']
      capabilities = {}
      capabilities['tunnel-identifier'] = os.environ['TRAVIS_JOB_NUMBER']
      capabilities['build'] = os.environ['TRAVIS_BUILD_NUMBER']
      capabilities['platform'] = 'Linux'
      capabilities['browserName'] = 'chrome'
      hub_url = '%s:%s@localhost:4445' % (username, access_key)
      cls.driver = webdriver.Remote(
          desired_capabilities=capabilities,
          command_executor='http://%s/wd/hub' % hub_url)
    else:
      os.environ['webdriver.chrome.driver'] = os.path.join(
          os.environ['VTROOT'], 'dist')
      # Only testing against Chrome for now
      cls.driver = webdriver.Chrome()

    port = environment.reserve_ports(1)
    vttest_environment.base_port = port
    mysql_flavor.set_mysql_flavor(None)

    cls.db = local_database.LocalDatabase(
        topology, '', False, None,
        os.path.join(os.environ['VTTOP'], 'web/vtctld'),
        os.path.join(os.environ['VTTOP'], 'test/vttest_schema/default'))
    cls.db.setup()
    cls.vtctld_addr = 'http://localhost:%d' % cls.db.config()['port']
    utils.pause('Paused test after vtcombo was started.\n'
                'For manual testing, connect to vtctld: %s' % cls.vtctld_addr)

  @classmethod
  def tearDownClass(cls):
    cls.db.teardown()
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
    shard_grid = self.driver.find_element_by_id(
        '%s-shards-list' % keyspace_name)
    return shard_grid.text.split('\n')

  def _get_serving_shards(self, keyspace_name):
    serving_shards = self.driver.find_element_by_id(
        '%s-serving-list' % keyspace_name)
    return serving_shards.text.split('\n')

  def _get_inactive_shards(self, keyspace_name):
    inactive_shards = self.driver.find_element_by_id(
        '%s-inactive-list' % keyspace_name)
    return inactive_shards.text.split('\n')

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

    logging.info('Fetching main vtctld page: %s', self.vtctld_addr)
    self.driver.get(self.vtctld_addr)

    keyspace_names = self._get_keyspaces()
    logging.info('Keyspaces: %s', ', '.join(keyspace_names))
    self.assertListEqual(['test_keyspace', 'test_keyspace2'], keyspace_names)

    test_keyspace_serving_shards = self._get_serving_shards('test_keyspace')
    logging.info(
        'Serving Shards in test_keyspace: %s', ', '.join(
            test_keyspace_serving_shards))
    self.assertListEqual(test_keyspace_serving_shards, ['-80', '80-'])

    test_keyspace2_serving_shards = self._get_serving_shards('test_keyspace2')
    logging.info(
        'Serving Shards in test_keyspace2: %s', ', '.join(
            test_keyspace2_serving_shards))
    self.assertListEqual(test_keyspace2_serving_shards, ['0'])

    with self.assertRaises(NoSuchElementException):
      self._get_inactive_shards('test_keyspace')
      logging.info(
          'Inactive Shards in test_keyspace: %s', ', '.join([]))

    with self.assertRaises(NoSuchElementException):
      self._get_inactive_shards('test_keyspace2')
      logging.info(
          'Inactive Shards in test_keyspace2: %s', ', '.join([]))

  def test_shard_overview(self):
    logging.info('Testing shard overview')

    logging.info('Fetching main vtctld page: %s', self.vtctld_addr)
    self.driver.get(self.vtctld_addr)

    self._check_shard_overview(
        'test_keyspace', '-80', {'master': 1, 'replica': 1, 'rdonly': 2})
    self._check_shard_overview(
        'test_keyspace', '80-', {'master': 1, 'replica': 1, 'rdonly': 2})
    self._check_shard_overview(
        'test_keyspace2', '0', {'master': 1, 'replica': 1, 'rdonly': 1})


def add_test_options(parser):
  parser.add_option(
      '--no-xvfb', action='store_false', dest='xvfb', default=True,
      help='Use local DISPLAY instead of headless Xvfb mode.')


if __name__ == '__main__':
  utils.main(test_options=add_test_options)
