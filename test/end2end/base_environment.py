"""Base environment for full end2end tests.

Contains functions that all environments should implement along with functions
common to all environments.
"""

import json
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
      VitessEnvironmentError: Raised if unsupported
    """
    raise VitessEnvironmentError(
        'Create unsupported in this environment')

  def use_named(self, instance_name):
    """Populate this instance based on a pre-existing environment.

    Args:
      instance_name: Name of the existing environment instance (string)
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
        'Destroy unsupported in this environment')

  def get_vtgate_conn(self, cell):
    """Gets a connection to a vtgate in a particular cell.

    Args:
      cell: cell to obtain a vtgate connection from (string)

    Returns:
      A vtgate connection.

    Raises:
      VitessEnvironmentError: Raised if unsupported
    """
    raise VitessEnvironmentError(
        'Get VTGate Conn unsupported in this environment')

  def restart_mysql_task(
      self, cell, keyspace, shard, task_num, dbtype, task_name, is_alloc=False):
    """Restart a job within the mysql alloc or the whole alloc itself.

    Args:
      cell: cell value containing the vttablet alloc to restart (string).
      keyspace: keyspace (string).
      shard: shard number (int).
      task_num: which vttablet alloc task to restart (int).
      dbtype: which dbtype to restart (replica | rdonly) (string).
      task_name: Name of specific  task (droid, vttablet, mysql, etc.)
      is_alloc: True to restart entire alloc

    Returns:
      return restart return val

    Raises:
      VitessEnvironmentError: Raised if unsupported
    """
    raise VitessEnvironmentError(
        'Restart MySQL task unsupported in this environment')

  def wait_for_good_failover_status(
      self, keyspace, shard_name, failover_completion_timeout_s=60):
    """Wait until failover status shows complete.

    Repeatedly queries the master tablet for failover status until it is 'OFF'.
    Most of the time the failover status check will immediately pass.  When a
    failover is in progress, it tends to take a good 5 to 10 attempts before
    status is 'OFF'.

    Args:
      keyspace: Name of the keyspace to reparent (string)
      shard_name: name of the shard to verify (e.g. '-80') (string)
      failover_completion_timeout_s: Failover completion timeout (int)

    Raises:
      VitessEnvironmentError: Raised if unsupported
    """
    raise VitessEnvironmentError(
        'Wait for good failover status unsupported in this environment')

  def wait_for_healthy_tablets(self):
    """Wait until all tablets report healthy status.

    Raises:
      VitessEnvironmentError: Raised if unsupported
    """
    raise VitessEnvironmentError(
        'Wait for healthy tablets unsupported in this environment')

  def get_next_master(self, keyspace, shard_name, cross_cell=False):
    """Determine what instance to select as the next master.

    If the next master is cross-cell, rotate the master cell and use instance 0
    as the master.  Otherwise, rotate the instance number.

    Args:
      keyspace: the name of the keyspace to reparent (string).
      shard_name: name of the shard to reparent (string).
      cross_cell: Whether the desired reparent is to another cell (bool).

    Returns:
      Tuple of cell, task num, tablet uid (string, int, string)
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
      tablet_name: Name of the tablet (string)

    Returns:
      0 based task number (int).

    Raises:
      VitessEnvironmentError: Raised if unsupported
    """
    raise VitessEnvironmentError(
        'Get tablet task number unsupported in this environment')

  def external_reparent(self, keyspace, new_cell, shard, new_task_num):
    """Perform a reparent through external means (Orchestrator, etc.).

    Args:
      keyspace: name of the keyspace to reparent (string)
      new_cell: new master cell (string)
      shard: 0 based shard index to reparent (int)
      new_task_num: 0 based task num to become next master (int)

    Raises:
      VitessEnvironmentError: Raised if unsupported
    """
    raise VitessEnvironmentError(
        'External reparent unsupported in this environment')

  def internal_reparent(self, keyspace, new_master_uid, emergency=False):
    raise VitessEnvironmentError(
        'Internal reparent unsupported in this environment')

  def get_current_master_name(self, keyspace, shard_name):
    """Obtains current master's tablet name (cell-uid).

    Args:
      keyspace: name of the keyspace to get information on the master
      shard_name: string representation of the shard in question (e.g. '-80')

    Returns:
      master tablet name (cell-uid) (string)
    """
    shard_info = json.loads(self.vtctl_helper.execute_vtctl_command(
        ['GetShard', '{0}/{1}'.format(keyspace, shard_name)]))
    master_alias = shard_info['master_alias']
    return '%s-%s' % (master_alias['cell'], master_alias['uid'])

  def get_tablet_cell(self, tablet_name):
    """Get the cell of a tablet.

    Args:
      tablet_name: Name of the tablet, including cell prefix. (string)

    Returns:
      Tablet's cell. (string)
    """
    return tablet_name.split('-')[0]

  def get_tablet_uid(self, tablet_name):
    """Get the uid of a tablet.

    Args:
      tablet_name: Name of the tablet, including cell prefix. (string)

    Returns:
      Tablet's uid. (int)
    """
    return int(tablet_name.split('-')[-1])

  def get_tablet_shard(self, tablet_name):
    """Get the shard of a tablet.

    Args:
      tablet_name: Name of the tablet, including cell prefix. (string)

    Returns:
      Tablet's shard. (string)
    """
    return json.loads(self.vtctl_helper.execute_vtctl_command(
        ['GetTablet', tablet_name]))['shard']

  def get_tablet_type(self, tablet_name):
    """Get the current type of the tablet as reported via vtctl.

    Args:
      tablet_name: Name of the tablet, including cell prefix. (string)

    Returns:
      Current tablet type (e.g. spare, replica, rdonly). (string)
    """
    return json.loads(self.vtctl_helper.execute_vtctl_command(
        ['GetTablet', tablet_name]))['type']

  def get_tablet_ip_port(self, tablet_name):
    """Get the ip and port of the tablet as reported via vtctl.

    Args:
      tablet_name: Name of the tablet, including cell prefix. (string)

    Returns:
      ip:port (string)
    """
    tablet_info = json.loads(self.vtctl_helper.execute_vtctl_command(
        ['GetTablet', tablet_name]))
    return '%s:%s' % (tablet_info['ip'], tablet_info['port_map']['vt'])

  def get_tablet_types_for_shard(self, keyspace, shard_name):
    """Get the types for all tablets in a shard.

    Args:
      keyspace: Name of keyspace to get tablet information on. (string)
      shard_name: single shard to obtain tablet types from (string)

    Returns:
      List of pairs of tablet's name and type
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
      keyspace: Name of keyspace to get tablet information on. (string)
      num_shards: number of shards in the keyspace. (int)

    Returns:
      List of pairs of tablet's name and type
    """
    tablet_info = []
    for shard_name in sharding_utils.get_shard_names(num_shards):
      tablet_info += self.get_tablet_types_for_shard(keyspace, shard_name)
    return tablet_info
