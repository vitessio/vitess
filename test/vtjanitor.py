#!/usr/bin/python
import datetime
import logging
import os
import time
import unittest

import environment
import tablet
import utils

old_master = tablet.Tablet()
new_master = tablet.Tablet()
drifter = tablet.Tablet()
good_replica = tablet.Tablet()

tablets = [old_master, new_master, drifter, good_replica]


def setUpModule():
  try:
    environment.topo_server_setup()

    setup_procs = [t.init_mysql() for t in tablets]
    utils.wait_procs(setup_procs)
    for t in tablets:
      t.start_mysql()
      t.create_db('vt_test_keyspace')

  except:
    tearDownModule()
    raise


def tearDownModule():
  if utils.options.skip_teardown:
    return

  teardown_procs = [t.teardown_mysql() for t in tablets]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  for t in tablets:
    t.remove_tree()

class TestCase(unittest.TestCase):

  def assertEqualAfter(self, deadline_seconds, fun, want, *args, **kwargs):
    """Succeeds if an assertion becomes true before the deadline is reached.

    assertEqualAfter will succeed if at some point before
    deadline_seconds pass, fun() becomes equal to want.

    """
    step = 0.01
    deadline = datetime.datetime.now() + datetime.timedelta(seconds=deadline_seconds)
    while True:
      try:
        self.assertEqual(fun(), want, *args, **kwargs)
      except AssertionError:
        if datetime.datetime.now() > deadline:
          raise
        time.sleep(step)
        step *= 1.3
      else:
        return



class Vtjanitor(object):

  def __init__(self, module_name, active):
    self.port = environment.reserve_ports(1)
    args = [environment.binary_path('vtjanitor'),
            '-keyspace', 'test_keyspace',
            '-shard', '0',
            '-sleep_time', '1s',
            '-log_dir', environment.vtlogroot,
            '-port', str(self.port)]
    args.extend(environment.topo_server_flags() +
                environment.tablet_manager_protocol_flags())
    if active:
      args.extend(['-active_modules', module_name])
    else:
      args.extend(['-dry_run_modules', module_name])

    stderr_fd = open(os.path.join(environment.tmproot, 'vtjanitor.stderr'), 'w')
    self.proc = utils.run_bg(args, stderr=stderr_fd)

  def drifting_tablets(self):
    return sum(self.vars()['DriftingTabletsCount'].values())

  def vars(self):
    return utils.get_vars(int(self.port))

  def close(self):
    self.proc.kill()


class TestDrifter(TestCase):

  def start_drifter(self, active=False):
    try:
      self.janitor.close()
    except AttributeError:
      pass
    self.janitor = Vtjanitor('drifter', active)
    utils.wait_for_vars('vtjanitor', self.janitor.port)

  def setUp(self):
    utils.run_vtctl('CreateKeyspace test_keyspace')

    old_master.init_tablet(
        'master', 'test_keyspace', '0', start=True, wait_for_start=True)
    for t in new_master, drifter, good_replica:
      t.init_tablet(
          'replica', 'test_keyspace', '0', start=True, wait_for_start=True,)
    utils.run_vtctl(
        ['ReparentShard', '-force', 'test_keyspace/0', old_master.tablet_alias])
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)
    self.start_drifter()

  def tearDown(self):
    self.janitor.close()

  def test(self):
    logging.debug("old master: %s, new master: %s, drifter: %s",
                  old_master.tablet_alias, new_master.tablet_alias, drifter.tablet_alias)
    # Make the new_master the new master.
    new_master.mquery('', [
        'reset master',
        'stop slave',
        'reset slave',
        "change master to master_host = ''",
    ])
    new_pos = new_master.mquery('', 'show master status')

    # Manually reparent the old master to the new master.

    old_master.mquery('', [
        'reset master',
        'reset slave',
        ("change master to master_host='%s', master_port=%u, "
         "master_log_file='%s', master_log_pos=%u") % (
             utils.hostname,
             new_master.mysql_port,
             new_pos[0][0],
             new_pos[0][1]
      )
    ])

    good_replica.mquery('', [
        'stop slave',
        'reset slave',
        ("change master to master_host='%s', master_port=%u, "
         "master_log_file='%s', master_log_pos=%u") % (
             utils.hostname,
             new_master.mysql_port,
             new_pos[0][0],
             new_pos[0][1]
      )
    ])

    utils.run_vtctl(['ShardExternallyReparented',
                     '-accept-success-percents=10',
                     'test_keyspace/0',
                     new_master.tablet_alias])
    # Now the drifter has finally got the delayed external reparent.
    drifter.mquery('', [
        'stop slave',
        'reset slave',
        ("change master to master_host='%s', master_port=%u, "
         "master_log_file='%s', master_log_pos=%u") % (
             utils.hostname,
             new_master.mysql_port,
             new_pos[0][0],
             new_pos[0][1]
      )
    ])

    self.assertEqualAfter(2, self.janitor.drifting_tablets, 1)


    self.start_drifter(active=True)
    self.assertEqualAfter(4, self.janitor.drifting_tablets, 0)

if __name__ == '__main__':
  utils.main()
