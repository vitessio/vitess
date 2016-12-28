"""Base environment for full cluster tests.

Contains functions that all environments should implement along with functions
common to all environments.
"""

# pylint: disable=unused-argument

import json
import random
from vttest import sharding_utils


class VitessEnvironmentError(Exception):
  pass


class BaseEnvironment(object):
  """Base Environment."""

  def __init__(self):
    self.vtctl_helper = None

  def create(self, **kwargs):
    """Create the environment.

    Args:
      **kwargs: kwargs parameterizing the environment.

    Raises:
      VitessEnvironmentError: Raised if unsupported.
    """
    raise VitessEnvironmentError(
        'create unsupported in this environment')

  def use_named(self, instance_name):
    """Populate this instance based on a pre-existing environment.

    Args:
      instance_name: Name of the existing environment instance (string).
    """
    self.master_capable_tablets = {}
    for keyspace, num_shards in zip(self.keyspaces, self.num_shards):
      self.master_capable_tablets[keyspace] = {}
      for shard_name in sharding_utils.get_shard_names(num_shards):
        raw_shard_tablets = self.vtctl_helper.execute_vtctl_command(
            ['ListShardTablets', '%s/%s' % (keyspace, shard_name)])
        split_shard_tablets = [
            t.split(' ') for t in raw_shard_tablets.split('\n') if t]
        self.master_capable_tablets[keyspace][shard_name] = [
            t[0] for t in split_shard_tablets
            if (self.get_tablet_cell(t[0]) in self.primary_cells
                and (t[3] == 'master' or t[3] == 'replica'))]

  def destroy(self):
    """Teardown the environment.

    Raises:
      VitessEnvironmentError: Raised if unsupported
    """
    raise VitessEnvironmentError(
        'destroy unsupported in this environment')

  def create_table(self, table_name, schema=None, validate_deadline_s=60):
    schema = schema or (
        'create table %s (id bigint auto_increment, msg varchar(64), '
        'keyspace_id bigint(20) unsigned NOT NULL, primary key (id)) '
        'Engine=InnoDB' % table_name)
    for keyspace in self.keyspaces:
      self.vtctl_helper.execute_vtctl_command(
          ['ApplySchema', '-sql', schema, keyspace])

  def delete_table(self, table_name):
    for keyspace in self.keyspaces:
      self.vtctl_helper.execute_vtctl_command(
          ['ApplySchema', '-sql', 'drop table if exists %s' % table_name,
           keyspace])

  def get_vtgate_conn(self, cell):
    """Gets a connection to a vtgate in a particular cell.

    Args:
      cell: cell to obtain a vtgate connection from (string).

    Returns:
      A vtgate connection.

    Raises:
      VitessEnvironmentError: Raised if unsupported.
    """
    raise VitessEnvironmentError(
        'get_vtgate_conn unsupported in this environment')

  def restart_mysql_task(self, tablet_name, task_name, is_alloc=False):
    """Restart a job within the mysql alloc or the whole alloc itself.

    Args:
      tablet_name: tablet associated with the mysql instance (string).
      task_name: Name of specific task (droid, vttablet, mysql, etc.).
      is_alloc: True to restart entire alloc.

    Returns:
      return restart return val.

    Raises:
      VitessEnvironmentError: Raised if unsupported.
    """
    raise VitessEnvironmentError(
        'restart_mysql_task unsupported in this environment')

  def restart_vtgate(self, cell=None, task_num=None):
    """Restarts a vtgate task.

    If cell and task_num are unspecified, restarts a random task in a random
    cell.

    Args:
      cell: cell containing the vtgate task to restart (string).
      task_num: which vtgate task to restart (int).

    Returns:
      return val for restart.

    Raises:
      VitessEnvironmentError: Raised if unsupported.
    """
    raise VitessEnvironmentError(
        'restart_vtgate unsupported in this environment')

  def wait_for_good_failover_status(
      self, keyspace, shard_name, failover_completion_timeout_s=60):
    """Wait until failover status shows complete.

    Repeatedly queries the master tablet for failover status until it is 'OFF'.
    Most of the time the failover status check will immediately pass.  When a
    failover is in progress, it tends to take a good 5 to 10 attempts before
    status is 'OFF'.

    Args:
      keyspace: Name of the keyspace to reparent (string).
      shard_name: name of the shard to verify (e.g. '-80') (string).
      failover_completion_timeout_s: Failover completion timeout (int).

    Raises:
      VitessEnvironmentError: Raised if unsupported.
    """
    raise VitessEnvironmentError(
        'wait_for_good_failover_status unsupported in this environment')

  def wait_for_healthy_tablets(self, deadline_s=300):
    """Wait until all tablets report healthy status.

    Args:
      deadline_s: Deadline timeout (seconds) (int).

    Raises:
      VitessEnvironmentError: Raised if unsupported.
    """
    raise VitessEnvironmentError(
        'wait_for_healthy_tablets unsupported in this environment')

  def is_tablet_healthy(self, tablet_name):
    vttablet_stream_health = json.loads(self.vtctl_helper.execute_vtctl_command(
        ['VtTabletStreamHealth', tablet_name]))
    return 'health_error' not in vttablet_stream_health['realtime_stats']

  def get_next_master(self, keyspace, shard_name, cross_cell=False):
    """Determine what instance to select as the next master.

    If the next master is cross-cell, rotate the master cell and use instance 0
    as the master.  Otherwise, rotate the instance number.

    Args:
      keyspace: the name of the keyspace to reparent (string).
      shard_name: name of the shard to reparent (string).
      cross_cell: Whether the desired reparent is to another cell (bool).

    Returns:
      Tuple of cell, task num, tablet uid (string, int, string).
    """
    num_tasks = self.keyspace_alias_to_num_instances_dict[keyspace]['replica']
    current_master = self.get_current_master_name(keyspace, shard_name)
    current_master_cell = self.get_tablet_cell(current_master)
    next_master_cell = current_master_cell
    next_master_task = 0
    if cross_cell:
      next_master_cell = self.primary_cells[(
          self.primary_cells.index(current_master_cell) + 1) % len(
              self.primary_cells)]
    else:
      next_master_task = (
          (self.get_tablet_task_number(current_master) + 1) % num_tasks)
    tablets_in_cell = [tablet for tablet in
                       self.master_capable_tablets[keyspace][shard_name]
                       if self.get_tablet_cell(tablet) == next_master_cell]
    return (next_master_cell, next_master_task,
            tablets_in_cell[next_master_task])

  def get_tablet_task_number(self, tablet_name):
    """Gets a tablet's 0 based task number.

    Args:
      tablet_name: Name of the tablet (string).

    Returns:
      0 based task number (int).

    Raises:
      VitessEnvironmentError: Raised if unsupported.
    """
    raise VitessEnvironmentError(
        'get_tablet_task_number unsupported in this environment')

  def external_reparent(self, keyspace, shard_name, new_master_name):
    """Perform a reparent through external means (Orchestrator, etc.).

    Args:
      keyspace: name of the keyspace to reparent (string).
      shard_name: shard name (string).
      new_master_name: tablet name of the tablet to become master (string).

    Raises:
      VitessEnvironmentError: Raised if unsupported.
    """
    raise VitessEnvironmentError(
        'external_reparent unsupported in this environment')

  def internal_reparent_available(self):
    """Checks if the environment can do a vtctl reparent."""
    return 'PlannedReparentShard' in (
        self.vtctl_helper.execute_vtctl_command(['help']))

  def automatic_reparent_available(self):
    """Checks if the environment can automatically reparent."""
    return False

  def explicit_external_reparent_available(self):
    """Checks if the environment can explicitly reparent via external tools."""
    return False

  def internal_reparent(self, keyspace, shard_name, new_master_name,
                        emergency=False):
    """Performs an internal reparent through vtctl.

    Args:
      keyspace: name of the keyspace to reparent (string).
      shard_name: string representation of the shard to reparent (e.g. '-80').
      new_master_name: Name of the new master tablet (string).
      emergency: True to perform an emergency reparent (bool).
    """
    reparent_type = (
        'EmergencyReparentShard' if emergency else 'PlannedReparentShard')
    self.vtctl_helper.execute_vtctl_command(
        [reparent_type, '%s/%s' % (keyspace, shard_name), new_master_name])
    self.vtctl_helper.execute_vtctl_command(['RebuildKeyspaceGraph', keyspace])

  def get_current_master_cell(self, keyspace):
    """Obtains current master cell.

    This gets the master cell for the first shard in the keyspace, and assumes
    that all shards share the same master.

    Args:
      keyspace: name of the keyspace to get the master cell for (string).

    Returns:
      master cell name (string).
    """
    num_shards = self.num_shards[self.keyspaces.index(keyspace)]
    first_shard_name = sharding_utils.get_shard_name(0, num_shards)
    first_shard_master_tablet = (
        self.get_current_master_name(keyspace, first_shard_name))
    return self.get_tablet_cell(first_shard_master_tablet)

  def get_current_master_name(self, keyspace, shard_name):
    """Obtains current master's tablet name (cell-uid).

    Args:
      keyspace: name of the keyspace to get information on the master.
      shard_name: string representation of the shard in question (e.g. '-80').

    Returns:
      master tablet name (cell-uid) (string).
    """
    shard_info = json.loads(self.vtctl_helper.execute_vtctl_command(
        ['GetShard', '{0}/{1}'.format(keyspace, shard_name)]))
    master_alias = shard_info['master_alias']
    return '%s-%s' % (master_alias['cell'], master_alias['uid'])

  def get_random_tablet(self, keyspace=None, shard_name=None, cell=None,
                        tablet_type=None, task_number=None):
    """Get a random tablet name.

    Args:
      keyspace: name of the keyspace to get information on the master.
      shard_name: shard to select tablet from (None for random) (string).
      cell: cell to select tablet from (None for random) (string).
      tablet_type: type of tablet to select (None for random) (string).
      task_number: a specific task number (None for random) (int).

    Returns:
      random tablet name (cell-uid) (string).
    """
    keyspace = keyspace or random.choice(self.keyspaces)
    shard_name = shard_name or (
        sharding_utils.get_shard_name(
            random.randint(0, self.shards[self.keyspaces.index(keyspace)])))
    cell = cell or random.choice(self.cells)
    tablets = [t.split(' ') for t in self.vtctl_helper.execute_vtctl_command(
        ['ListShardTablets', '%s/%s' % (keyspace, shard_name)]).split('\n')]
    cell_tablets = [t for t in tablets if self.get_tablet_cell(t[0]) == cell]
    if task_number:
      return cell_tablets[task_number][0]
    if tablet_type:
      return random.choice([t[0] for t in cell_tablets if t[3] == tablet_type])
    return random.choice(cell_tablets)[0]

  def get_tablet_cell(self, tablet_name):
    """Get the cell of a tablet.

    Args:
      tablet_name: Name of the tablet, including cell prefix (string).

    Returns:
      Tablet's cell (string).
    """
    return tablet_name.split('-')[0]

  def get_tablet_uid(self, tablet_name):
    """Get the uid of a tablet.

    Args:
      tablet_name: Name of the tablet, including cell prefix (string).

    Returns:
      Tablet's uid (int).
    """
    return int(tablet_name.split('-')[-1])

  def get_tablet_keyspace(self, tablet_name):
    """Get the keyspace of a tablet.

    Args:
      tablet_name: Name of the tablet, including cell prefix (string).

    Returns:
      Tablet's keyspace (string).
    """
    return json.loads(self.vtctl_helper.execute_vtctl_command(
        ['GetTablet', tablet_name]))['keyspace']

  def get_tablet_shard(self, tablet_name):
    """Get the shard of a tablet.

    Args:
      tablet_name: Name of the tablet, including cell prefix (string).

    Returns:
      Tablet's shard (string).
    """
    return json.loads(self.vtctl_helper.execute_vtctl_command(
        ['GetTablet', tablet_name]))['shard']

  def get_tablet_type(self, tablet_name):
    """Get the current type of the tablet as reported via vtctl.

    Args:
      tablet_name: Name of the tablet, including cell prefix (string).

    Returns:
      Current tablet type (e.g. spare, replica, rdonly) (string).
    """
    return json.loads(self.vtctl_helper.execute_vtctl_command(
        ['GetTablet', tablet_name]))['type']

  def get_tablet_ip_port(self, tablet_name):
    """Get the ip and port of the tablet as reported via vtctl.

    Args:
      tablet_name: Name of the tablet, including cell prefix (string).

    Returns:
      ip:port (string).
    """
    tablet_info = json.loads(self.vtctl_helper.execute_vtctl_command(
        ['GetTablet', tablet_name]))
    return '%s:%s' % (tablet_info['ip'], tablet_info['port_map']['vt'])

  def get_tablet_types_for_shard(self, keyspace, shard_name):
    """Get the types for all tablets in a shard.

    Args:
      keyspace: Name of keyspace to get tablet information on (string).
      shard_name: single shard to obtain tablet types from (string).

    Returns:
      List of pairs of tablet's name and type.
    """
    tablet_info = []
    raw_tablets = self.vtctl_helper.execute_vtctl_command(
        ['ListShardTablets', '{0}/{1}'.format(keyspace, shard_name)])
    raw_tablets = filter(None, raw_tablets.split('\n'))
    for tablet in raw_tablets:
      tablet_words = tablet.split()
      tablet_name = tablet_words[0]
      tablet_type = tablet_words[3]
      tablet_info.append((tablet_name, tablet_type))
    return tablet_info

  def get_all_tablet_types(self, keyspace, num_shards):
    """Get the types for all tablets in a keyspace.

    Args:
      keyspace: Name of keyspace to get tablet information on (string).
      num_shards: number of shards in the keyspace (int).

    Returns:
      List of pairs of tablet's name and type.
    """
    tablet_info = []
    for shard_name in sharding_utils.get_shard_names(num_shards):
      tablet_info += self.get_tablet_types_for_shard(keyspace, shard_name)
    return tablet_info

  def backup(self, tablet_name):
    """Wait until all tablets report healthy status.

    Args:
      tablet_name: Name of tablet to backup (string).

    Raises:
      VitessEnvironmentError: Raised if unsupported.
    """
    raise VitessEnvironmentError(
        'backup unsupported in this environment')

  def drain_tablet(self, tablet_name, duration_s=600):
    """Add a drain from a tablet.

    Args:
      tablet_name: vttablet to drain (string).
      duration_s: how long to have the drain exist for, in seconds (int).

    Raises:
      VitessEnvironmentError: Raised if unsupported.
    """
    raise VitessEnvironmentError(
        'drain_tablet unsupported in this environment')

  def is_tablet_drained(self, tablet_name):
    """Checks whether a tablet is drained.

    Args:
      tablet_name: vttablet to drain (string).

    Raises:
      VitessEnvironmentError: Raised if unsupported.
    """
    raise VitessEnvironmentError(
        'is_tablet_drained unsupported in this environment')

  def undrain_tablet(self, tablet_name):
    """Remove a drain from a tablet.

    Args:
      tablet_name: vttablet name to undrain (string).

    Raises:
      VitessEnvironmentError: Raised if unsupported.
    """
    raise VitessEnvironmentError(
        'undrain_tablet unsupported in this environment')

  def is_tablet_undrained(self, tablet_name):
    """Checks whether a tablet is undrained.

    Args:
      tablet_name: vttablet to undrain (string).

    Raises:
      VitessEnvironmentError: Raised if unsupported.
    """
    raise VitessEnvironmentError(
        'is_tablet_undrained unsupported in this environment')

  def poll_for_varz(self, tablet_name, varz, timeout=60.0,
                    condition_fn=None, converter=str, condition_msg=None):
    """Polls for varz to exist, or match specific conditions, within a timeout.

    Args:
      tablet_name: the name of the process that we're trying to poll vars from.
      varz: name of the vars to fetch from varz.
      timeout: number of seconds that we should attempt to poll for.
      condition_fn: a function that takes the var as input, and returns a truthy
        value if it matches the success conditions.
      converter: function to convert varz value.
      condition_msg: string describing the conditions that we're polling for,
        used for error messaging.

    Returns:
      dict of requested varz.

    Raises:
      VitessEnvironmentError: Raised if unsupported or if the varz conditions
        aren't met within the given timeout.
    """
    raise VitessEnvironmentError(
        'poll_for_varz unsupported in this environment')

  def truncate_usertable(self, keyspace, shard, table=None):
    tablename = table or self.tablename
    master_tablet = self.get_current_master_name(keyspace, shard)
    self.vtctl_helper.execute_vtctl_command(
        ['ExecuteFetchAsDba', master_tablet, 'truncate %s' % tablename])

  def get_tablet_query_total_count(self, tablet_name):
    """Gets the total query count of a specified tablet.

    Args:
      tablet_name: Name of the tablet to get query count from (string).

    Returns:
      Query total count (int).

    Raises:
      VitessEnvironmentError: Raised if unsupported.
    """
    raise VitessEnvironmentError(
        'get_tablet_query_total_count unsupported in this environment')

