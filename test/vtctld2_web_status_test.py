#!/usr/bin/env python
"""A vtctld2 webdriver test."""

import logging
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support.ui import Select
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
    topology.cells.append('test2')
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

  def _get_dropdown_options(self, group):
    status_content = self.driver.find_element_by_tag_name('vt-status')
    dropdown = status_content.find_element_by_id(group)
    return [op.text for op in
            dropdown.find_elements_by_tag_name('option')]

  def _get_dropdown_selection(self, group):
    status_content = self.driver.find_element_by_tag_name('vt-status')
    dropdown = status_content.find_element_by_id(group)
    return dropdown.find_element_by_tag_name('label').text

  def _change_dropdown_option(self, dropdownId, dropdownValue):
    status_content = self.driver.find_element_by_tag_name('vt-status')
    dropdown = status_content.find_element_by_id(dropdownId)
    dropdown.click()
    options = dropdown.find_elements_by_tag_name('li')
    for op in options:
      if op.text == dropdownValue:
        op.click()
        break

  def _check_dropdowns(self, keyspaces, selected_keyspace, cells, selected_cell,
                       types, selected_type, metrics, selected_metric):
    keyspace_options = self._get_dropdown_options('keyspace')
    keyspace_selected = self._get_dropdown_selection('keyspace')
    logging.info('Keyspace options: %s Keyspace selected: %s',
                 ', '.join(keyspace_options), keyspace_selected)
    self.assertListEqual(keyspaces, keyspace_options)
    self.assertEqual(selected_keyspace, keyspace_selected)

    cell_options = self._get_dropdown_options('cell')
    cell_selected = self._get_dropdown_selection('cell')
    logging.info('Cell options: %s Cell Selected: %s',
                 ', '.join(cell_options), cell_selected)
    self.assertListEqual(cells, cell_options)
    self.assertEqual(selected_cell, cell_selected)

    type_options = self._get_dropdown_options('type')
    type_selected = self._get_dropdown_selection('type')
    logging.info('Type options: %s Type Selected: %s',
                 ', '.join(cell_options), cell_selected)
    self.assertListEqual(types, type_options)
    self.assertEqual(selected_type, type_selected)

    metric_options = self._get_dropdown_options('metric')
    metric_selected = self._get_dropdown_selection('metric')
    logging.info('metric options: %s metric Selected: %s',
                 ', '.join(metric_options), metric_selected)
    self.assertListEqual(metrics, metric_options)
    self.assertEqual(selected_metric, metric_selected)

  # Responsible for checking the number of heatmaps is as expected and that the
  # title is correct as well as the div for the map is properly drawn.
  def _check_heatmaps(self, selected_keyspace):
    status_content = self.driver.find_element_by_tag_name('vt-status')
    keyspaces = status_content.find_elements_by_tag_name('vt-heatmap')
    logging.info('Number of keyspaces found: %d', len(keyspaces))
    if selected_keyspace == "all":
      available_keyspaces = self._get_dropdown_options('keyspace')
      self.assertEqual(len(keyspaces), len(available_keyspaces)-1)
      for ks in keyspaces:
        heading = ks.find_element_by_id("keyspaceName")
        logging.info('Keyspace name: %s', heading.text)
        present = False
        try:
          ks.find_element_by_id(heading.text);
          present = True
        except:
          present = False
        self.assertTrue(present);
        self.assertIn(heading.text, available_keyspaces)
    else:
      self.assertEquals(len(keyspaces), 1)
      heading = keyspaces[0].find_element_by_id('keyspaceName')
      logging.info('Keyspace name: %s', heading.text)
      present = False
      try:
        keyspaces[0].find_element_by_id(heading.text);
        present = True
      except:
        present = False
      self.assertTrue(present);
      self.assertEquals(heading.text, selected_keyspace)

  # Responsible for checking the dropdowns and heatmaps for each newly routed
  # views.
  def _check_new_view(
      self, keyspaces, selected_keyspace, cells, selected_cell, types,
      selected_type, metrics, selected_metric):
    logging.info('Testing realtime stats view')
    self._check_dropdowns(keyspaces, selected_keyspace, cells, selected_cell,
                          types, selected_type, metrics, selected_metric)
    self._check_heatmaps(selected_keyspace)

  def test_realtime_stats(self):
    logging.info('Testing realtime stats view')

    # Navigate to the status page from intial app.
    self.driver.get(self.vtctld_addr)
    statusButton = self.driver.find_element_by_partial_link_text('Status')
    statusButton.click()
    wait = WebDriverWait(self.driver, 10)
    wait.until(expected_conditions.visibility_of_element_located(
        (By.TAG_NAME, 'vt-status')))

    self._check_new_view(keyspaces = ['test_keyspace', 'test_keyspace2', 'all'],
                         selected_keyspace = 'all',
                         cells = ['test', 'test2', 'all'],
                         selected_cell = 'all',
                         types = ['MASTER', 'REPLICA', 'RDONLY', 'all'],
                         selected_type = 'all',
                         metrics = ['lag', 'qps', 'health'],
                         selected_metric = 'health'
                        )

    logging.info('Routing to all-all-REPLICA view')
    self._change_dropdown_option('type', 'REPLICA')
    self._check_new_view(keyspaces = ['test_keyspace', 'test_keyspace2', 'all'],
                         selected_keyspace = 'all',
                         cells = ['test', 'test2', 'all'],
                         selected_cell = 'all',
                         types = ['MASTER', 'REPLICA', 'RDONLY', 'all'],
                         selected_type = 'REPLICA',
                         metrics = ['lag', 'qps', 'health'],
                         selected_metric = 'health'
                        )

    logging.info('Routing to all-test2-REPLICA')
    self._change_dropdown_option('cell', 'test2')
    self._check_new_view(keyspaces = ['test_keyspace', 'test_keyspace2', 'all'],
                         selected_keyspace = 'all',
                         cells = ['test', 'test2', 'all'],
                         selected_cell = 'test2',
                         types = ['REPLICA', 'RDONLY', 'all'],
                         selected_type = 'REPLICA',
                         metrics = ['lag', 'qps', 'health'],
                         selected_metric = 'health'
                        )

    logging.info('Routing to test_keyspace-test2-REPLICA')
    self._change_dropdown_option('keyspace', 'test_keyspace')
    self._check_new_view(keyspaces = ['test_keyspace', 'test_keyspace2', 'all'],
                         selected_keyspace = 'test_keyspace',
                         cells = ['test', 'test2', 'all'],
                         selected_cell = 'test2',
                         types = ['REPLICA', 'RDONLY', 'all'],
                         selected_type = 'REPLICA',
                         metrics = ['lag', 'qps', 'health'],
                         selected_metric = 'health'
                        )
    logging.info('Routing to test_keyspace-all-REPLICA')
    self._change_dropdown_option('cell', 'all')
    self._check_new_view(keyspaces = ['test_keyspace', 'test_keyspace2', 'all'],
                         selected_keyspace = 'test_keyspace',
                         cells = ['test', 'test2', 'all'],
                         selected_cell = 'all',
                         types = ['MASTER', 'REPLICA', 'RDONLY', 'all'],
                         selected_type = 'REPLICA',
                         metrics = ['lag', 'qps', 'health'],
                         selected_metric = 'health'
                        )

    logging.info('Routing to test_keyspace-all-all')
    self._change_dropdown_option('type', 'all')
    self._check_new_view(keyspaces = ['test_keyspace', 'test_keyspace2', 'all'],
                         selected_keyspace = 'test_keyspace',
                         cells = ['test', 'test2', 'all'],
                         selected_cell = 'all',
                         types = ['MASTER', 'REPLICA', 'RDONLY', 'all'],
                         selected_type = 'all',
                         metrics = ['lag', 'qps', 'health'],
                         selected_metric = 'health'
                        )

    logging.info('Routing to test_keyspace-test2-all')
    self._change_dropdown_option('cell', 'test2')
    self._check_new_view(keyspaces = ['test_keyspace', 'test_keyspace2', 'all'],
                         selected_keyspace = 'test_keyspace',
                         cells = ['test', 'test2', 'all'],
                         selected_cell = 'test2',
                         types = ['REPLICA', 'RDONLY', 'all'],
                         selected_type = 'all',
                         metrics = ['lag', 'qps', 'health'],
                         selected_metric = 'health'
                        )

    logging.info('Routing to all-test2-all')
    self._change_dropdown_option('keyspace', 'all')
    self._check_new_view(keyspaces = ['test_keyspace', 'test_keyspace2', 'all'],
                         selected_keyspace = 'all',
                         cells = ['test', 'test2', 'all'],
                         selected_cell = 'test2',
                         types = ['REPLICA', 'RDONLY', 'all'],
                         selected_type = 'all',
                         metrics = ['lag', 'qps', 'health'],
                         selected_metric = 'health'
                        )


def add_test_options(parser):
  parser.add_option(
      '--no-xvfb', action='store_false', dest='xvfb', default=True,
      help='Use local DISPLAY instead of headless Xvfb mode.')


if __name__ == '__main__':
  utils.main(test_options=add_test_options)
