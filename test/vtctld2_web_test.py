#!/usr/bin/env python
"""A vtctld2 webdriver test."""

import logging
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
import unittest

from vtproto import vttest_pb2
from vttest import environment as vttest_environment
from vttest import local_database
from vttest import mysql_flavor

import environment
import utils


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

    port = environment.reserve_ports(1)
    vttest_environment.base_port = port
    mysql_flavor.set_mysql_flavor(None)

    cls.db = local_database.LocalDatabase(
        topology, '', False, None,
        os.path.join(os.environ['VTTOP'], 'web/vtctld2/dist'),
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
    wait = WebDriverWait(self.driver, 10)
    wait.until(expected_conditions.visibility_of_element_located(
        (By.TAG_NAME, 'vt-dashboard')))
    dashboard_content = self.driver.find_element_by_tag_name('vt-dashboard')
    return [ks.text for ks in
            dashboard_content.find_elements_by_tag_name('md-card-title')]

  def test_dashboard(self):
    logging.info('Testing dashboard view')

    logging.info('Fetching main vtctld page: %s', self.vtctld_addr)
    self.driver.get(self.vtctld_addr)

    keyspace_names = self._get_keyspaces()
    logging.info('Keyspaces: %s', ', '.join(keyspace_names))
    self.assertListEqual(['test_keyspace', 'test_keyspace2'], keyspace_names)


def add_test_options(parser):
  parser.add_option(
      '--no-xvfb', action='store_false', dest='xvfb', default=True,
      help='Use local DISPLAY instead of headless Xvfb mode.')


if __name__ == '__main__':
  utils.main(test_options=add_test_options)
