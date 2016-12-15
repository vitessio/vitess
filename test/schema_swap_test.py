#!/usr/bin/env python
import json
import logging
import re
import time
import unittest
import urllib2

from vtproto import topodata_pb2

import environment
import tablet
import utils


# range '' - 80
shard_0_master = tablet.Tablet(use_mysqlctld=True)
shard_0_replica = tablet.Tablet(use_mysqlctld=True)
shard_0_rdonly = tablet.Tablet(use_mysqlctld=True)
all_shard_0_tablets = [shard_0_master, shard_0_replica, shard_0_rdonly]
# range 80 - ''
shard_1_master = tablet.Tablet(use_mysqlctld=True)
shard_1_replica = tablet.Tablet(use_mysqlctld=True)
shard_1_rdonly = tablet.Tablet(use_mysqlctld=True)
all_shard_1_tablets = [shard_1_master, shard_1_replica, shard_1_rdonly]
# all tablets
all_tablets = all_shard_0_tablets + all_shard_1_tablets


def setUpModule():
  try:
    environment.topo_server().setup()

    for t in all_tablets:
      t.init_mysql()
    utils.Vtctld().start()
    for t in all_tablets:
      t.wait_for_mysqlctl_socket()
  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return

  teardown_procs = [t.teardown_mysql() for t in all_tablets]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  for t in all_tablets:
    t.remove_tree()


class TestSchemaSwap(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    cls._start_tablets('-80',
                       [shard_0_master, shard_0_replica],
                       [shard_0_rdonly])
    cls._start_tablets('80-',
                       [shard_1_master, shard_1_replica],
                       [shard_1_rdonly])

    for t in all_tablets:
      t.wait_for_vttablet_state('NOT_SERVING')

    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/-80',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/80-',
                     shard_1_master.tablet_alias], auto_log=True)

    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    for t in all_tablets:
      t.wait_for_vttablet_state('SERVING')

  @classmethod
  def _start_tablets(cls, shard_name, replica_tablets, rdonly_tablets):
    """Start all tablets on a shard.

    Args:
      shard_name: string, name of the shard passed to the tablet.
      replica_tablets: list of tablet.Tablet, list of tablets that should be
          started as replica.
      rdonly_tablets: list of tablet.Tablet, list of tablets that should be
          started as rdonly.
    """
    for t in replica_tablets:
      t.start_vttablet(wait_for_state=None,
                       init_tablet_type='replica',
                       init_keyspace='test_keyspace',
                       init_shard=shard_name,
                       extra_args=utils.vtctld.process_args())
    for t in rdonly_tablets:
      t.start_vttablet(wait_for_state=None,
                       init_tablet_type='rdonly',
                       init_keyspace='test_keyspace',
                       init_shard=shard_name,
                       extra_args=utils.vtctld.process_args())

  create_table_sql = ('DROP TABLE IF EXISTS test; '
                      'CREATE TABLE test (id int, PRIMARY KEY(id)) '
                      'Engine=InnoDB')
  schema_swap_sql = 'ALTER TABLE test ADD COLUMN (t TEXT)'
  show_schema_sql = 'SHOW CREATE TABLE test'
  schema_check_string = '`t` text,'

  def _check_final_schema(self):
    """Check that schema of test table is correct after a successful swap."""
    schema_0 = shard_0_master.mquery('vt_test_keyspace',
                                     self.show_schema_sql)[0][1]
    schema_1 = shard_1_master.mquery('vt_test_keyspace',
                                     self.show_schema_sql)[0][1]
    self.assertEqual(schema_0, schema_1)
    self.assertIn(self.schema_check_string, schema_0)

  def setUp(self):
    utils.run_vtctl(['ApplySchema',
                     '-sql=%s' % self.create_table_sql,
                     'test_keyspace'],
                    auto_log=True)

    for t in [shard_0_master, shard_1_master]:
      tablet_info = utils.run_vtctl_json(['GetTablet', t.tablet_alias])
      if tablet_info['type'] != topodata_pb2.MASTER:
        utils.run_vtctl(['InitShardMaster', '-force',
                         'test_keyspace/' + t.shard, t.tablet_alias],
                        auto_log=True)
        tablet_info = utils.run_vtctl_json(['GetTablet', t.tablet_alias])
        self.assertEqual(tablet_info['type'], topodata_pb2.MASTER)

      t.mquery('_vt', "DELETE FROM shard_metadata where name in ("
                      "'LastStartedSchemaSwap','LastFinishedSchemaSwap',"
                      "'CurrentSchemaSwapSQL');"
                      "DELETE FROM local_metadata "
                      "where name = 'LastAppliedSchemaSwap';")

    self._vtctld_url = 'http://localhost:%d' % utils.vtctld.port
    self._wait_for_functional_vtctld()
    self._start_vtctld_long_poll()

  def _start_swap(self, sql):
    """Start a new schema swap with the given SQL statement."""
    self._swap_error = None
    vtctl_res = utils.run_vtctl(['WorkflowCreate',
                                 'schema_swap',
                                 '-keyspace=test_keyspace',
                                 '-sql=%s' % sql],
                                auto_log=True)
    m = re.match(r'^uuid: (.*)$', vtctl_res[0])
    return m.group(1)

  def _stop_swap(self, swap_uuid):
    """Stop the running schema swap with the given uuid."""
    utils.run_vtctl(['WorkflowStop', swap_uuid], auto_log=True)

  def _delete_swap(self, swap_uuid):
    """Delete the schema swap with the given uuid."""
    utils.run_vtctl(['WorkflowDelete', swap_uuid], auto_log=True)

  def _fetch_json_from_vtctld(self, url_path):
    """Fetch and deserialize a json object from vtctld.

    Args:
      url_path: string, a path appended to vtctld address to create a URL that
         is used to fetch json object from.
    Returns:
      deserialized json object returned from vtctld.
    """
    full_url = '%s/%s' % (self._vtctld_url, url_path)
    f = urllib2.urlopen(full_url)
    res_json = f.read()
    f.close()
    return json.loads(res_json)

  def _start_vtctld_long_poll(self):
    """Start long polling of workflow updates from vtctld."""
    poll_update = self._fetch_json_from_vtctld('api/workflow/create')
    self._poll_id = poll_update['index']
    return poll_update

  def _wait_for_functional_vtctld(self):
    """Wait until vtctld is fully up and is able to respond to polls."""
    while True:
      try:
        poll_update = self._fetch_json_from_vtctld('api/workflow/create')
        if poll_update.get('index') is None:
          time.sleep(0.1)
          continue
        break
      except urllib2.HTTPError:
        pass

  def _send_retry_vtctld_action(self, swap_uuid):
    """Emulate click of the Retry button on the schema swap."""
    req = urllib2.Request('%s/api/workflow/action/%s' %
                          (self._vtctld_url, self._poll_id))
    req.add_header('Content-Type', 'application/json; charset=utf-8')
    resp = urllib2.urlopen(req, '{"path":"/%s","name":"Retry"}' % swap_uuid)
    logging.info('Retry response code: %r', resp.getcode())

  def _strip_logs_from_nodes(self, nodes):
    """Strip all the logs from the node hierarchy."""
    for node in nodes:
      if node.get('log'):
        del node['log']
      if node.get('children'):
        self._strip_logs_from_nodes(node['children'])

  def _poll_vtctld(self):
    """Do one poll of vtctld for updates to workflow UI.

    If for any reason the poll breaks the method tries to restart the long
    polling.

    Returns:
      deserialized json object that came from vtctld as the result of poll. Can
      be an incremental or a full update.
    """
    try:
      poll_update = self._fetch_json_from_vtctld('api/workflow/poll/%s' %
                                                 self._poll_id)
    except urllib2.HTTPError as e:
      logging.info('Error polling vtctld, will try to re-create the long poll: '
                   '%s', e)
      poll_update = self._start_vtctld_long_poll()

    if poll_update.get('nodes'):
      # Log contents in the nodes is very big and makes our test logs very hard
      # to read without bringing any new information (the history of actions is
      # already present in test logs through the logging of incremental polls
      # that is done here). Because of that we are stripping all the log
      # contents from the nodes hierarchy.
      self._strip_logs_from_nodes(poll_update['nodes'])
    logging.info('Workflow polling update: %r', poll_update)

    return poll_update

  def _has_swap_done_or_error(self, nodes, swap_uuid):
    """Check if the node list has root node of the swap that is finished.

    Args:
      nodes: list, list of nodes that came in an update from vtctld.
      swap_uuid: string, uuid of the swap to look for.
    Returns:
      bool, whether the list of nodes had the root node for the swap and the
      swap was finished. When True is returned self._swap_error will contain
      the error or success message displayed in the swap root node.
    """
    for node in nodes:
      if node['pathName'] == swap_uuid:
        if node['actions'] or node['state'] == 2:
          # Button Retry appeared or state is 'Done'. Then the 'message' will
          # have the error.
          self._swap_error = node['message']
          return True
        # Other nodes are not interesting.
        break

    return False

  def _wait_for_success_or_error(self, swap_uuid, reset_error=False):
    """Wait until schema swap finishes successfully or with error.

    Args:
      swap_uuid: string, uuid of the schema swap to wait for.
      reset_error: bool, should be set to True when the swap already had an
          error and we need to wait for the next one.
    Returns:
      string, error or success message displayed on the schema swap.
    """
    if reset_error:
      self._swap_error = None
    # Error can have been seen already during execution of
    # _wait_for_progress_message().
    if self._swap_error is not None:
      return self._swap_error

    while True:
      poll_update = self._poll_vtctld()
      if not poll_update.get('nodes'):
        continue

      if self._has_swap_done_or_error(poll_update['nodes'], swap_uuid):
        return self._swap_error

  def _has_progress_message(self, nodes, message):
    """Check if any node in the hierarchy has the given progress message."""
    for node in nodes:
      if node.get('progressMsg') == message:
        return True
      children = node.get('children')
      if children and self._has_progress_message(children, message):
        return True

    return False

  def _wait_for_progress_message(self, swap_uuid, message):
    """Wait until at least one node has the given progress message.

    The method returns when some node has the given progress message or when
    the given swap has finished successfully or with an error. The latter is
    necessary to not wait forever if the swap finishes without ever having the
    given progress message.

    Args:
      swap_uuid: string, uuid of the swap being waited for.
      message: string, progress message to wait for.
    """
    while True:
      poll_update = self._poll_vtctld()
      if not poll_update.get('nodes'):
        continue

      if self._has_progress_message(poll_update['nodes'], message):
        return
      if self._has_swap_done_or_error(poll_update['nodes'], swap_uuid):
        return

  def test_successful_swap(self):
    """Normal swap running from start to finish, "happy path"."""
    swap_uuid = self._start_swap(self.schema_swap_sql)
    err = self._wait_for_success_or_error(swap_uuid)
    self.assertEqual(err, 'Schema swap is finished')
    self._check_final_schema()
    self._delete_swap(swap_uuid)

  def test_restarted_swap(self):
    """Force a restart of schema swap in the middle."""
    swap_uuid = self._start_swap(self.schema_swap_sql)
    # Wait until at least one tablet has the new schema (the progress message is
    # '1/3') and then forcefully stop the swap.
    self._wait_for_progress_message(swap_uuid, '1/3')
    self._stop_swap(swap_uuid)
    err = self._wait_for_success_or_error(swap_uuid)
    self.assertIn('context canceled', err)
    self._delete_swap(swap_uuid)

    # While we are at it try to start new swap with a different SQL statement.
    # The swap should fail.
    swap_uuid = self._start_swap('ALTER TABLE test ADD COLUMN i int')
    err = self._wait_for_success_or_error(swap_uuid)
    self.assertIn('different set of SQL statements', err)
    self._stop_swap(swap_uuid)
    self._delete_swap(swap_uuid)

    # Now restart with the correct statement and should succeed.
    swap_uuid = self._start_swap(self.schema_swap_sql)
    err = self._wait_for_success_or_error(swap_uuid)
    self.assertEqual(err, 'Schema swap is finished')
    self._check_final_schema()
    self._delete_swap(swap_uuid)

  def _retry_or_restart_swap(self, swap_uuid, use_retry):
    """Click Retry button on the swap or fully restart it.

    Args:
      swap_uuid: string, uuid of the schema swap to restart.
      use_retry: bool, if True then Retry button is clicked, if False then the
          swap is restarted completely as a new workflow.
    Returns:
      string, uuid of the new swap if it's restarted, or swap_uuid if the swap
      was retried.
    """
    if use_retry:
      self._send_retry_vtctld_action(swap_uuid)
    else:
      self._stop_swap(swap_uuid)
      self._delete_swap(swap_uuid)
      swap_uuid = self._start_swap(self.schema_swap_sql)
    return swap_uuid

  def _test_init_error(self, use_retry):
    """Schema swap interrupted by an error during initialization."""
    # By marking the master read-only we cause an error when schema swap tries
    # to write shard metadata during initialization.
    shard_1_master.mquery('', 'SET GLOBAL read_only = 1')
    swap_uuid = self._start_swap(self.schema_swap_sql)
    err = self._wait_for_success_or_error(swap_uuid)
    self.assertIn('running with the --read-only option', err)

    shard_1_master.mquery('', 'SET GLOBAL read_only = 0')
    swap_uuid = self._retry_or_restart_swap(swap_uuid, use_retry=use_retry)
    err = self._wait_for_success_or_error(swap_uuid, reset_error=True)
    self.assertEqual(err, 'Schema swap is finished')
    self._check_final_schema()
    self._delete_swap(swap_uuid)

  def test_init_error_with_retry(self):
    self._test_init_error(use_retry=True)

  def test_init_error_with_restart(self):
    self._test_init_error(use_retry=False)

  def _test_apply_error(self, use_retry):
    """Schema swap interrupted while applying seed schema change."""
    # Renaming the test table to cause ALTER TABLE executed during schema swap
    # to fail.
    shard_1_master.mquery('vt_test_keyspace', 'RENAME TABLE test TO test2')
    swap_uuid = self._start_swap(self.schema_swap_sql)
    err = self._wait_for_success_or_error(swap_uuid)
    self.assertIn("Table 'vt_test_keyspace.test' doesn't exist", err)

    shard_1_master.mquery('vt_test_keyspace', 'RENAME TABLE test2 TO test')
    swap_uuid = self._retry_or_restart_swap(swap_uuid, use_retry=use_retry)
    err = self._wait_for_success_or_error(swap_uuid, reset_error=True)
    self.assertEqual(err, 'Schema swap is finished')
    self._check_final_schema()
    self._delete_swap(swap_uuid)

  def test_apply_error_with_retry(self):
    self._test_apply_error(use_retry=True)

  def test_apply_error_with_restart(self):
    self._test_apply_error(use_retry=False)

  def _restart_vtctld(self, extra_flags):
    """Restart vtctld possibly passing it some additional flags.

    The method makes sure that restarted vtctld has the same listening port as
    the one that was before.

    Args:
      extra_flags: list of strings, list of additional flags to pass to vtctld
    """
    vtctld_port = utils.vtctld.port
    utils.vtctld.proc.terminate()
    utils.vtctld.proc.wait()
    utils.vtctld = None
    new_vtctld = utils.Vtctld()
    new_vtctld.port = vtctld_port
    new_vtctld.start(extra_flags=extra_flags)
    self._wait_for_functional_vtctld()

  def test_reparent_error(self):
    """Schema swap interrupted by an error during reparent."""
    # With -disable_active_reparents and without 'reparent_away' hook on
    # vttablet the attempt to reparent during schema swap will always fail.
    self._restart_vtctld(extra_flags=['-disable_active_reparents'])

    swap_uuid = self._start_swap(self.schema_swap_sql)
    err = self._wait_for_success_or_error(swap_uuid)
    self.assertIn("Error executing 'reparent_away'", err)

    self._restart_vtctld(extra_flags=[])
    # We don't need to restart the swap here because it's automatically
    # restarted by vtctld when it's started.
    err = self._wait_for_success_or_error(swap_uuid, reset_error=True)
    self.assertEqual(err, 'Schema swap is finished')
    self._check_final_schema()
    self._delete_swap(swap_uuid)


if __name__ == '__main__':
  utils.main()
