#!/usr/bin/env python

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

"""A vtctld webdriver test."""

import logging
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
import unittest

from vtproto import vttest_pb2
from vttest import environment as vttest_environment
from vttest import local_database

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


class TestVtctldWeb(unittest.TestCase):

  WEBDRIVER_TIMEOUT_S = 10

  @classmethod
  def setUpClass(cls):
    """Set up two keyspaces: one unsharded, one with two shards."""
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

    cls.driver = environment.create_webdriver()

    port = environment.reserve_ports(1)
    vttest_environment.base_port = port

    environment.reset_mysql_flavor()

    cls.db = local_database.LocalDatabase(
        topology,
        os.path.join(environment.vttop, 'test/vttest_schema'),
        False, None,
        web_dir=os.path.join(environment.vttop, 'web/vtctld'),
        default_schema_dir=os.path.join(
            environment.vttop, 'test/vttest_schema/default'),
        web_dir2=os.path.join(environment.vttop, 'web/vtctld2/app'))
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
    element_id = '%s-card' % keyspace_name
    wait = WebDriverWait(self.driver, self.WEBDRIVER_TIMEOUT_S)
    wait.until(expected_conditions.visibility_of_element_located(
        (By.ID, element_id)))
    return self.driver.find_element_by_id(element_id)

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
    wait = WebDriverWait(self.driver, self.WEBDRIVER_TIMEOUT_S)
    wait.until(expected_conditions.visibility_of_element_located(
        (By.ID, 'keyspace-shard')))
    return self.driver.find_element_by_id('keyspace-shard').text

  def _get_shard_record_master_tablet(self):
    return self.driver.find_element_by_id('master-tablet').text

  def _check_tablet_types(self, tablet_types, expected_counts):
    for expected_type, count in expected_counts.items():
      self.assertEqual(count,
                        len([x for x in tablet_types if x == expected_type]))

  def _check_shard_overview(
      self, keyspace_name, shard_name, expected_tablet_types):
    logging.info('Checking %s/%s', keyspace_name, shard_name)
    self._get_shard_element(keyspace_name, shard_name).click()
    self.assertEqual(self._get_shard_record_keyspace_shard(),
                      '%s/%s' % (keyspace_name, shard_name))
    master = self._get_shard_record_master_tablet()
    logging.info('master tablet is %s', master)
    shard_tablets = self._get_tablet_names()
    self.assertEqual(shard_tablets[master], 'master')
    self._check_tablet_types(list(shard_tablets.values()), expected_tablet_types)
    self.driver.back()

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
      self.assertEqual(len(keyspaces), 1)
      heading = keyspaces[0].find_element_by_id('keyspaceName')
      logging.info('Keyspace name: %s', heading.text)
      try:
        keyspaces[0].find_element_by_id(heading.text)
      except NoSuchElementException:
        self.fail('Cannot get keyspace')
      self.assertEqual(heading.text, selected_keyspace)

  def _check_new_view(
      self, keyspaces, selected_keyspace, cells, selected_cell, types,
      selected_type, metrics, selected_metric):
    """Checking the dropdowns and heatmaps for each newly routed view."""
    logging.info('Testing realtime stats view')
    self._check_dropdowns(keyspaces, selected_keyspace, cells, selected_cell,
                          types, selected_type, metrics, selected_metric)
    self._check_heatmaps(selected_keyspace)

  # Navigation
  def _navigate_to_dashboard(self):
    logging.info('Fetching main vtctld page: %s', self.vtctld_addr)
    self.driver.get('%s/app2' % self.vtctld_addr)
    wait = WebDriverWait(self.driver, self.WEBDRIVER_TIMEOUT_S)
    wait.until(expected_conditions.visibility_of_element_located(
        (By.ID, 'test_keyspace')))

  def _navigate_to_keyspace_view(self):
    self._navigate_to_dashboard()
    dashboard_content = self.driver.find_element_by_tag_name('vt-dashboard')
    keyspace_cards = dashboard_content.find_elements_by_class_name('vt-card')
    self.assertEqual(2, len(keyspace_cards))

    first_keyspace_card = keyspace_cards[0]
    shard_stats = first_keyspace_card.find_element_by_tag_name('md-list')
    shard_stats.click()
    wait = WebDriverWait(self.driver, self.WEBDRIVER_TIMEOUT_S)
    wait.until(expected_conditions.visibility_of_element_located(
        (By.CLASS_NAME, 'vt-card')))

  def _navigate_to_shard_view(self):
    self._navigate_to_keyspace_view()
    keyspace_content = self.driver.find_element_by_tag_name('vt-keyspace-view')
    shard_cards = keyspace_content.find_elements_by_class_name(
        'vt-serving-shard')
    self.assertEqual(2, len(shard_cards))

    first_shard_card = shard_cards[0]
    first_shard_card.click()
    wait = WebDriverWait(self.driver, self.WEBDRIVER_TIMEOUT_S)
    wait.until(expected_conditions.visibility_of_element_located(
        (By.ID, '1')))

  # Get Elements
  def _get_dashboard_keyspaces(self):
    """Get list of all present keyspaces."""
    wait = WebDriverWait(self.driver, self.WEBDRIVER_TIMEOUT_S)
    wait.until(expected_conditions.visibility_of_element_located(
        (By.TAG_NAME, 'vt-dashboard')))
    dashboard_content = self.driver.find_element_by_tag_name('vt-dashboard')
    return [ks.text for ks in
            dashboard_content.find_elements_by_class_name('vt-keyspace-card')]

  def _get_dashboard_shards(self):
    """Get list of all present shards."""
    wait = WebDriverWait(self.driver, self.WEBDRIVER_TIMEOUT_S)
    wait.until(expected_conditions.visibility_of_element_located(
        (By.TAG_NAME, 'vt-dashboard')))
    dashboard_content = self.driver.find_element_by_tag_name('vt-dashboard')
    return [sh.text for sh in
            dashboard_content.find_elements_by_class_name('vt-shard-stats')]

  def _get_keyspace_shards(self):
    wait = WebDriverWait(self.driver, self.WEBDRIVER_TIMEOUT_S)
    wait.until(expected_conditions.visibility_of_element_located(
        (By.TAG_NAME, 'vt-keyspace-view')))
    keyspace_content = self.driver.find_element_by_tag_name('vt-keyspace-view')
    return [sh.text for sh in
            keyspace_content.find_elements_by_class_name('vt-serving-shard')]

  def _get_shard_tablets(self):
    wait = WebDriverWait(self.driver, self.WEBDRIVER_TIMEOUT_S)
    wait.until(expected_conditions.visibility_of_element_located(
        (By.TAG_NAME, 'vt-shard-view')))
    shard_content = self.driver.find_element_by_tag_name('vt-shard-view')

    # Ignore Header row.
    tablet_types = []
    tablet_uids = []
    table_rows = shard_content.find_elements_by_tag_name('tr')[1:]
    for row in table_rows:
      columns = row.find_elements_by_tag_name('td')
      tablet_types.append(
          columns[1].find_element_by_class_name('ui-cell-data').text)
      tablet_uids.append(
          columns[3].find_element_by_class_name('ui-cell-data').text)
    return (tablet_types, tablet_uids)

  def _get_first_option(self, dashboard_content):
    dashboard_menu = dashboard_content.find_element_by_class_name('vt-menu')
    dashboard_menu.click()
    first_option = dashboard_content.find_element_by_class_name(
        'ui-menuitem-text')
    return first_option

  def _get_dialog_cmd(self, dialog):
    dialog_command = [
        cmd.text for cmd  in dialog.find_elements_by_class_name('vt-sheet')]
    return dialog_command

  def _toggle_dialog_checkbox(self, dialog, index):
    ping_tablets_checkbox = dialog.find_elements_by_class_name(
        'md-checkbox-inner-container')[index]
    ping_tablets_checkbox.click()

  def _get_validate_resp(self, dialog):
    validate = dialog.find_element_by_id('vt-action')
    validate.click()
    validate_response = dialog.find_element_by_class_name('vt-resp').text
    return validate_response

  def _close_dialog(self, dialog):
    dismiss = dialog.find_element_by_id('vt-dismiss')
    dismiss.click()

  def test_old_keyspace_overview(self):
    logging.info('Testing old keyspace overview')

    logging.info('Fetching main vtctld page: %s', self.vtctld_addr)
    self.driver.get(self.vtctld_addr + '/app')

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

  def test_old_shard_overview(self):
    logging.info('Testing old shard overview')

    logging.info('Fetching main vtctld page: %s', self.vtctld_addr)
    self.driver.get(self.vtctld_addr + '/app')

    self._check_shard_overview(
        'test_keyspace', '-80', {'master': 1, 'replica': 1, 'rdonly': 2})
    self._check_shard_overview(
        'test_keyspace', '80-', {'master': 1, 'replica': 1, 'rdonly': 2})
    self._check_shard_overview(
        'test_keyspace2', '0', {'master': 1, 'replica': 1, 'rdonly': 1})

  def test_dashboard(self):
    logging.info('Testing dashboard view')

    self._navigate_to_dashboard()

    keyspace_names = self._get_dashboard_keyspaces()
    shard_names = self._get_dashboard_shards()
    logging.info('Keyspaces: %s', ', '.join(keyspace_names))
    logging.info('Shards: %s', ', '.join(shard_names))
    self.assertListEqual(['test_keyspace', 'test_keyspace2'], keyspace_names)
    self.assertListEqual(['2 Shards', '1 Shards'], shard_names)

  def test_dashboard_validate(self):
    self._navigate_to_dashboard()
    dashboard_content = self.driver.find_element_by_tag_name('vt-dashboard')
    first_menu_option = self._get_first_option(dashboard_content)
    logging.info('First option of Dashboard menu: %s', first_menu_option.text)
    self.assertEqual('Validate', first_menu_option.text)

    first_menu_option.click()
    dialog = dashboard_content.find_element_by_tag_name('vt-dialog')
    dialog_command = self._get_dialog_cmd(dialog)
    logging.info('Validate command: %s', ', '.join(dialog_command))
    self.assertEqual(2, len(dialog_command))
    self.assertListEqual(['Validate', '-ping-tablets=false'], dialog_command)

    # Validate Dialog Checkbox is working
    self._toggle_dialog_checkbox(dialog, 0)
    dialog_command = self._get_dialog_cmd(dialog)
    logging.info('Validate command: %s', ', '.join(dialog_command))
    self.assertEqual(2, len(dialog_command))
    self.assertEqual('-ping-tablets', dialog_command[1])

    # Validate succeeded
    validate_response = self._get_validate_resp(dialog)
    logging.info('Validate command response: %s', validate_response)
    self._close_dialog(dialog)

  def test_create_keyspace(self):
    self._navigate_to_dashboard()
    dashboard_content = self.driver.find_element_by_tag_name('vt-dashboard')
    dialog = dashboard_content.find_element_by_tag_name('vt-dialog')
    # Create Keyspace Dialog command responds to name.
    dashboard_menu = dashboard_content.find_element_by_class_name('vt-menu')
    dashboard_menu.click()
    dashboard_menu_options = (
        dashboard_content.find_elements_by_class_name('ui-menuitem-text'))
    new_keyspace_option = [
        x for x in dashboard_menu_options if x.text == 'New'][0]
    new_keyspace_option.click()

    input_fields = [md_input.find_element_by_tag_name('input') for md_input in
                    dialog.find_elements_by_tag_name('md-input')]
    keyspace_name_field = input_fields[0]
    sharding_col_name_field = input_fields[1]
    keyspace_name_field.send_keys('test_keyspace3')
    dialog_command = [
        cmd.text for cmd  in dialog.find_elements_by_class_name('vt-sheet')]
    logging.info('Create keyspace command: %s', ', '.join(dialog_command))
    self.assertEqual(3, len(dialog_command))
    self.assertListEqual(['CreateKeyspace', '-force=false', 'test_keyspace3'],
                         dialog_command)

    # Create Keyspace autopopulates sharding_column type
    sharding_col_name_field.send_keys('test_id')
    dialog_command = [
        cmd.text for cmd  in dialog.find_elements_by_class_name('vt-sheet')]
    logging.info('Create keyspace command: %s', ', '.join(dialog_command))
    self.assertEqual(5, len(dialog_command))
    self.assertListEqual(['CreateKeyspace', '-sharding_column_name=test_id',
                          '-sharding_column_type=UINT64', '-force=false',
                          'test_keyspace3'],
                         dialog_command)

    # Dropdown works
    dropdown = dialog.find_element_by_tag_name('p-dropdown')
    dropdown.click()
    options = dropdown.find_elements_by_tag_name('li')
    options[1].click()
    dialog_command = [
        cmd.text for cmd  in dialog.find_elements_by_class_name('vt-sheet')]
    logging.info('Create keyspace command: %s', ', '.join(dialog_command))
    self.assertEqual(5, len(dialog_command))
    self.assertListEqual(['CreateKeyspace', '-sharding_column_name=test_id',
                          '-sharding_column_type=BYTES', '-force=false',
                          'test_keyspace3'],
                         dialog_command)

    create = dialog.find_element_by_id('vt-action')
    create.click()

    dismiss = dialog.find_element_by_id('vt-dismiss')
    dismiss.click()

    keyspace_names = self._get_dashboard_keyspaces()
    logging.info('Keyspaces: %s', ', '.join(keyspace_names))
    self.assertListEqual(
        ['test_keyspace', 'test_keyspace2', 'test_keyspace3'], keyspace_names)

    test_keyspace3 = dashboard_content.find_elements_by_class_name('vt-card')[2]
    test_keyspace3.find_element_by_class_name('vt-menu').click()
    options = test_keyspace3.find_elements_by_tag_name('li')

    delete = [x for x in options if x.text == 'Delete'][0]
    delete.click()

    delete = dialog.find_element_by_id('vt-action')
    delete.click()
    dismiss = dialog.find_element_by_id('vt-dismiss')
    dismiss.click()
    keyspace_names = self._get_dashboard_keyspaces()
    logging.info('Keyspaces: %s', ', '.join(keyspace_names))
    self.assertListEqual(['test_keyspace', 'test_keyspace2'], keyspace_names)

  def test_keyspace_view(self):
    self._navigate_to_keyspace_view()
    logging.info('Navigating to keyspace view')
    self._navigate_to_keyspace_view()
    logging.info('Testing keyspace view')
    shard_names = self._get_keyspace_shards()
    logging.info('Shards in first keyspace: %s', ', '.join(shard_names))
    self.assertListEqual(['-80', '80-'], shard_names)

  def test_shard_view(self):
    self._navigate_to_shard_view()
    logging.info('Navigating to shard view')
    self._navigate_to_shard_view()
    logging.info('Testing shard view')
    tablet_types, tablet_uids = self._get_shard_tablets()
    logging.info('Tablets types in first shard in first keyspace: %s',
                 ', '.join(tablet_types))
    logging.info('Tablets uids in first shard in first keyspace: %s',
                 ', '.join(tablet_uids))
    self.assertSetEqual(
        set(['master', 'replica', 'rdonly', 'rdonly', 'replica', 'replica',
             'rdonly', 'rdonly']), set(tablet_types))
    self.assertSetEqual(
        set(['1', '2', '3', '4', '5', '6', '7', '8']), set(tablet_uids))

  def test_realtime_stats(self):
    logging.info('Testing realtime stats view')

    # Navigate to the status page from initial app.
    # TODO(thompsonja): Fix this once direct navigation works (going to status
    # page directly should display correctly)
    self.driver.get('%s/app2' % self.vtctld_addr)
    status_button = self.driver.find_element_by_partial_link_text('Status')
    status_button.click()
    wait = WebDriverWait(self.driver, self.WEBDRIVER_TIMEOUT_S)
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
