#!/usr/bin/env python
"""A vtctld2 webdriver test."""

import logging
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
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
  utils.kill_sub_processes()


class TestVtctld2WebStatus(unittest.TestCase):

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
        topology,
        os.path.join(os.environ['VTTOP'], 'test/vttest_schema'),
        False, None,
        web_dir=os.path.join(os.environ['VTTOP'], 'web/vtctld'),
        default_schema_dir=os.path.join(
            os.environ['VTTOP'], 'test/vttest_schema/default'),
        web_dir2=os.path.join(os.environ['VTTOP'], 'web/vtctld2/app'))
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
    toolbars = dashboard_content.find_elements_by_class_name('vt-card-toolbar')
    return [t.find_element_by_class_name('vt-title').text for t in toolbars]

  def _get_dropdown_options(self, group):
    status_content = self.driver.find_element_by_tag_name('vt-status')
    dropdown = status_content.find_element_by_id(group)
    return [op.text for op in
            dropdown.find_elements_by_tag_name('option')]

  def _get_dropdown_selection(self, group):
    status_content = self.driver.find_element_by_tag_name('vt-status')
    dropdown = status_content.find_element_by_id(group)
    return dropdown.find_element_by_tag_name('label').text

  def _change_dropdown_option(self, dropdown_id, dropdown_value):
    status_content = self.driver.find_element_by_tag_name('vt-status')
    dropdown = status_content.find_element_by_id(dropdown_id)
    dropdown.click()
    options = dropdown.find_elements_by_tag_name('li')
    for op in options:
      if op.text == dropdown_value:
        logging.info('dropdown %s: option %s clicked', dropdown_id, op.text)
        op.click()
        break

  def _check_dropdowns(self, keyspaces, selected_keyspace, cells, selected_cell,
                       types, selected_type, metrics, selected_metric):
    """Checking that all dropdowns have the correct options and selection."""
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

  def _check_heatmaps(self, selected_keyspace):
    """Checking that the view has the correct number of heatmaps drawn."""
    status_content = self.driver.find_element_by_tag_name('vt-status')
    keyspaces = status_content.find_elements_by_tag_name('vt-heatmap')
    logging.info('Number of keyspaces found: %d', len(keyspaces))
    if selected_keyspace == 'all':
      available_keyspaces = self._get_dropdown_options('keyspace')
      self.assertEqual(len(keyspaces), len(available_keyspaces)-1)
      for ks in keyspaces:
        heading = ks.find_element_by_id('keyspaceName')
        logging.info('Keyspace name: %s', heading.text)
        try:
          ks.find_element_by_id(heading.text)
        except NoSuchElementException:
          self.fail('Cannot get keyspace')
        self.assertIn(heading.text, available_keyspaces)
    else:
      self.assertEquals(len(keyspaces), 1)
      heading = keyspaces[0].find_element_by_id('keyspaceName')
      logging.info('Keyspace name: %s', heading.text)
      try:
        keyspaces[0].find_element_by_id(heading.text)
      except NoSuchElementException:
        self.fail('Cannot get keyspace')
      self.assertEquals(heading.text, selected_keyspace)

  def _check_new_view(
      self, keyspaces, selected_keyspace, cells, selected_cell, types,
      selected_type, metrics, selected_metric):
    """Checking the dropdowns and heatmaps for each newly routed view."""
    logging.info('Testing realtime stats view')
    self._check_dropdowns(keyspaces, selected_keyspace, cells, selected_cell,
                          types, selected_type, metrics, selected_metric)
    self._check_heatmaps(selected_keyspace)

  def test_dashboard(self):
    logging.info('Testing dashboard view')

    logging.info('Fetching main vtctld page: %s/app2', self.vtctld_addr)
    self.driver.get('%s/app2' % self.vtctld_addr)

    keyspace_names = self._get_keyspaces()
    logging.info('Keyspaces: %s', ', '.join(keyspace_names))
    self.assertListEqual(['test_keyspace', 'test_keyspace2'], keyspace_names)

  def test_realtime_stats(self):
    logging.info('Testing realtime stats view')

    # Navigate to the status page from initial app.
    # TODO(thompsonja): Fix this once direct navigation works (going to status
    # page directly should display correctly)
    self.driver.get('%s/app2' % self.vtctld_addr)
    status_button = self.driver.find_element_by_partial_link_text('Status')
    status_button.click()
    wait = WebDriverWait(self.driver, 10)
    wait.until(expected_conditions.visibility_of_element_located(
        (By.TAG_NAME, 'vt-status')))

    test_cases = [
        (None, None, 'all', 'all', 'all'),
        ('type', 'REPLICA', 'all', 'all', 'REPLICA'),
        ('cell', 'test2', 'all', 'test2', 'REPLICA'),
        ('keyspace', 'test_keyspace', 'test_keyspace', 'test2', 'REPLICA'),
        ('cell', 'all', 'test_keyspace', 'all', 'REPLICA'),
        ('type', 'all', 'test_keyspace', 'all', 'all'),
        ('cell', 'test2', 'test_keyspace', 'test2', 'all'),
        ('keyspace', 'all', 'all', 'test2', 'all'),
    ]

    for (dropdown_id, dropdown_val, keyspace, cell, tablet_type) in test_cases:
      logging.info('Routing to new %s-%s-%s view', keyspace, cell, tablet_type)
      if dropdown_id and dropdown_val:
        self._change_dropdown_option(dropdown_id, dropdown_val)
      tablet_type_options = ['all', 'MASTER', 'REPLICA', 'RDONLY']
      if cell == 'test2':
        tablet_type_options = ['all', 'REPLICA', 'RDONLY']
      self._check_new_view(keyspaces=['all', 'test_keyspace', 'test_keyspace2'],
                           selected_keyspace=keyspace,
                           cells=['all', 'test', 'test2'],
                           selected_cell=cell,
                           types=tablet_type_options,
                           selected_type=tablet_type,
                           metrics=['lag', 'qps', 'health'],
                           selected_metric='health'
                          )


def add_test_options(parser):
  parser.add_option(
      '--no-xvfb', action='store_false', dest='xvfb', default=True,
      help='Use local DISPLAY instead of headless Xvfb mode.')


if __name__ == '__main__':
  utils.main(test_options=add_test_options)
