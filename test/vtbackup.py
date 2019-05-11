# Copyright 2019 The Vitess Authors
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
"""Manage VTBackup during test."""

import os
import logging
import shutil

import environment
from mysql_flavor import mysql_flavor
import utils

tablet_cell_map = {
    62344: 'nj',
    62044: 'nj',
    41983: 'nj',
    31981: 'ny',
}

def get_backup_storage_flags():
  return ['-backup_storage_implementation', 'file',
          '-file_backup_storage_root',
          os.path.join(environment.tmproot, 'backupstorage')]

def get_all_extra_my_cnf(extra_my_cnf):
  all_extra_my_cnf = [environment.vttop + '/config/mycnf/default-fast.cnf']
  flavor_my_cnf = mysql_flavor().extra_my_cnf()
  if flavor_my_cnf:
    all_extra_my_cnf.append(flavor_my_cnf)
  if extra_my_cnf:
    all_extra_my_cnf.append(extra_my_cnf)
  return all_extra_my_cnf


class Vtbackup(object):
  """This class helps manage a vtbackup instance.

  To use it for vtbackup, you need to use init_tablet and/or
  start_vtbackup.
  """
  default_uid = 65534
  seq = 0
  tablets_running = 0

  def __init__(self, tablet_uid=None, cell=None, vt_dba_passwd=None,
               mysql_port=None, use_mysqlctld=False):
    self.tablet_uid = tablet_uid or (Vtbackup.default_uid + Vtbackup.seq)
    self.mysql_port = mysql_port or (environment.reserve_ports(1))
    self.use_mysqlctld = use_mysqlctld
    self.vt_dba_passwd = vt_dba_passwd
    Vtbackup.seq += 1

    if cell:
      self.cell = cell
    else:
      self.cell = tablet_cell_map.get(tablet_uid, 'nj')
    self.proc = None

    # filled in during init_mysql
    self.mysqlctld_process = None

    # filled in during init_tablet
    self.keyspace = None
    self.shard = None
    self.index = None
    self.tablet_index = None
    # default to false
    self.external_mysql = False
    # utility variables
    self.tablet_alias = 'test_%s-%010d' % (self.cell, self.tablet_uid)

  def __str__(self):
    return 'vtbackup: uid: %d' % ( self.tablet_uid )

  def init_backup(self, keyspace, shard,
                  tablet_index=None,
                  start=False, dbname=None, parent=True, wait_for_start=True,
                  include_mysql_port=True, external_mysql=False, **kwargs):
    """Initialize a vtbackup."""

    self.keyspace = keyspace
    self.shard = shard
    self.tablet_index = tablet_index
    self.external_mysql = external_mysql

    self.dbname = dbname or ('vt_' + self.keyspace)

    if start:
      self.start_vtbackup(**kwargs)

  @property
  def tablet_dir(self):
    return '%s/vt_%010d' % (environment.vtdataroot, self.tablet_uid)

  def start_vtbackup(
      self, 
      full_mycnf_args=False,
      extra_args=None, extra_env=None, include_mysql_port=True,
      init_keyspace=None, init_shard=None,
      tablet_index=None,
      init_db_name_override=None):
    # pylint: disable=g-doc-args
    """Starts a vtprocess process, and returns it.

    The process is also saved in self.proc, so it's easy to kill as well.

    Returns:
      the process that was started.
    """
    # pylint: enable=g-doc-args

    args = environment.binary_args('vtbackup')
    # Use 'localhost' as hostname because Travis CI worker hostnames
    # are too long for MySQL replication.
    args.extend(['-tablet-path', self.tablet_alias])
    args.extend(environment.topo_server().flags())
    args.extend(['-pid_file', os.path.join(self.tablet_dir, 'vtbackup.pid')])
    # always enable_replication_reporter with somewhat short values for tests
    if self.use_mysqlctld:
      args.extend(
          ['-mysqlctl_socket', os.path.join(self.tablet_dir, 'mysqlctl.sock')])

    if self.external_mysql:
      args.extend(['-db_host', '127.0.0.1'])
      args.extend(['-db_port', str(self.mysql_port)])
      args.append('-disable_active_reparents')
    if full_mycnf_args:
      # this flag is used to specify all the mycnf_ flags, to make
      # sure that code works.
      relay_log_path = os.path.join(self.tablet_dir, 'relay-logs',
                                    'vt-%010d-relay-bin' % self.tablet_uid)
      args.extend([
          '-mycnf_server_id', str(self.tablet_uid),
          '-mycnf_data_dir', os.path.join(self.tablet_dir, 'data'),
          '-mycnf_innodb_data_home_dir', os.path.join(self.tablet_dir,
                                                      'innodb', 'data'),
          '-mycnf_innodb_log_group_home_dir', os.path.join(self.tablet_dir,
                                                           'innodb', 'logs'),
          '-mycnf_socket_file', os.path.join(self.tablet_dir, 'mysql.sock'),
          '-mycnf_error_log_path', os.path.join(self.tablet_dir, 'error.log'),
          '-mycnf_slow_log_path', os.path.join(self.tablet_dir,
                                               'slow-query.log'),
          '-mycnf_relay_log_path', relay_log_path,
          '-mycnf_relay_log_index_path', relay_log_path + '.index',
          '-mycnf_relay_log_info_path', os.path.join(self.tablet_dir,
                                                     'relay-logs',
                                                     'relay-log.info'),
          '-mycnf_bin_log_path', os.path.join(
              self.tablet_dir, 'bin-logs', 'vt-%010d-bin' % self.tablet_uid),
          '-mycnf_master_info_file', os.path.join(self.tablet_dir,
                                                  'master.info'),
          '-mycnf_pid_file', os.path.join(self.tablet_dir, 'mysql.pid'),
          '-mycnf_tmp_dir', os.path.join(self.tablet_dir, 'tmp'),
          '-mycnf_slave_load_tmp_dir', os.path.join(self.tablet_dir, 'tmp'),
      ])
      if include_mysql_port:
        args.extend(['-mycnf_mysql_port', str(self.mysql_port)])

    if init_keyspace:
      self.keyspace = init_keyspace
      self.shard = init_shard
      # tablet_index is required for the update_addr call below.
      if self.tablet_index is None:
        self.tablet_index = tablet_index
      args.extend(['-init_keyspace', init_keyspace,
                   '-init_shard', init_shard])
      if init_db_name_override:
        self.dbname = init_db_name_override
        args.extend(['-init_db_name_override', init_db_name_override])
      else:
        self.dbname = 'vt_' + init_keyspace

    # Backup Arguments are not optional
    args.extend(get_backup_storage_flags())

    # We need to have EXTRA_MY_CNF set properly.
    # When using mysqlctld, only mysqlctld should need EXTRA_MY_CNF.
    # If any test fails without giving EXTRA_MY_CNF to vtbackup,
    # it means we missed some call that should run remotely on mysqlctld.
    if not self.use_mysqlctld:
      all_extra_my_cnf = get_all_extra_my_cnf(None)
      if all_extra_my_cnf:
        if not extra_env:
          extra_env = {}
        extra_env['EXTRA_MY_CNF'] = ':'.join(all_extra_my_cnf)

    if extra_args:
      args.extend(extra_args)

    args.extend(['-log_dir', environment.vtlogroot])

    stderr_fd = open(
        os.path.join(environment.vtlogroot, 'vtbackup-%d.stderr' %
                     self.tablet_uid), 'w')
    # increment count only the first time
    if not self.proc:
      Vtbackup.tablets_running += 1
    self.proc = utils.run_bg(args, stderr=stderr_fd, extra_env=extra_env)

    log_message = (
        'Started vtbackup: %s (%s) with pid: %s - Log files: '
        '%s/vtbackup.*.{INFO,WARNING,ERROR,FATAL}.*.%s' %
        (self.tablet_uid, self.tablet_alias, self.proc.pid,
         environment.vtlogroot, self.proc.pid))
    # This may race with the stderr output from the process (though
    # that's usually empty).
    stderr_fd.write(log_message + '\n')
    stderr_fd.close()
    logging.debug(log_message)

    return self.proc

  def mysqlctl(self, cmd, extra_my_cnf=None, with_ports=False, verbose=False,
               extra_args=None):
    """Runs a mysqlctl command.

    Args:
      cmd: the command to run.
      extra_my_cnf: list of extra mycnf files to use
      with_ports: if set, sends the tablet and mysql ports to mysqlctl.
      verbose: passed to mysqlctld.
      extra_args: passed to mysqlctl.
    Returns:
      the result of run_bg.
    """
    extra_env = {}
    all_extra_my_cnf = get_all_extra_my_cnf(extra_my_cnf)
    if all_extra_my_cnf:
      extra_env['EXTRA_MY_CNF'] = ':'.join(all_extra_my_cnf)
    args = environment.binary_args('mysqlctl') + [
        '-log_dir', environment.vtlogroot,
        '-tablet_uid', str(self.tablet_uid)]
    if self.use_mysqlctld:
      args.extend(
          ['-mysqlctl_socket', os.path.join(self.tablet_dir, 'mysqlctl.sock')])
    if with_ports:
      args.extend(['-mysql_port', str(self.mysql_port)])
    if verbose:
      args.append('-alsologtostderr')
    if extra_args:
      args.extend(extra_args)
    args.extend(cmd)
    return utils.run_bg(args, extra_env=extra_env)  
  
  def mysqlctld(self, cmd, extra_my_cnf=None, verbose=False, extra_args=None):
    """Runs a mysqlctld command.

    Args:
      cmd: the command to run.
      extra_my_cnf: list of extra mycnf files to use
      verbose: passed to mysqlctld.
      extra_args: passed to mysqlctld.
    Returns:
      the result of run_bg.
    """
    extra_env = {}
    all_extra_my_cnf = get_all_extra_my_cnf(extra_my_cnf)
    if all_extra_my_cnf:
      extra_env['EXTRA_MY_CNF'] = ':'.join(all_extra_my_cnf)
    args = environment.binary_args('mysqlctld') + [
        '-log_dir', environment.vtlogroot,
        '-tablet_uid', str(self.tablet_uid),
        '-mysql_port', str(self.mysql_port),
        '-socket_file', os.path.join(self.tablet_dir, 'mysqlctl.sock')]
    if verbose:
      args.append('-alsologtostderr')
    if extra_args:
      args.extend(extra_args)
    args.extend(cmd)
    return utils.run_bg(args, extra_env=extra_env)
  
  def init_mysql(self, extra_my_cnf=None, init_db=None, extra_args=None,
                 use_rbr=False):
    """Init the mysql tablet directory, starts mysqld.

    Either runs 'mysqlctl init', or starts a mysqlctld process.

    Args:
      extra_my_cnf: to pass to mysqlctl.
      init_db: if set, use this init_db script instead of the default.
      extra_args: passed to mysqlctld / mysqlctl.
      use_rbr: configure the MySQL daemon to use RBR.

    Returns:
      The forked process.
    """
    if use_rbr:
      if extra_my_cnf:
        extra_my_cnf += ':' + environment.vttop + '/config/mycnf/rbr.cnf'
      else:
        extra_my_cnf = environment.vttop + '/config/mycnf/rbr.cnf'

    if not init_db:
      init_db = environment.vttop + '/config/init_db.sql'

    if self.use_mysqlctld:
      self.mysqlctld_process = self.mysqlctld(['-init_db_sql_file', init_db],
                                              extra_my_cnf=extra_my_cnf,
                                              extra_args=extra_args)
      return self.mysqlctld_process
    else:
      return self.mysqlctl(['init', '-init_db_sql_file', init_db],
                           extra_my_cnf=extra_my_cnf, with_ports=True,
                           extra_args=extra_args)

  def start_mysql(self):
    return self.mysqlctl(['start'], with_ports=True)

  def shutdown_mysql(self, extra_args=None):
    return self.mysqlctl(['shutdown'], with_ports=True, extra_args=extra_args)

  def teardown_mysql(self, extra_args=None):
    if self.use_mysqlctld and self.mysqlctld_process:
      # if we use mysqlctld, we just terminate it gracefully, so it kills
      # its mysqld. And we return it, so we can wait for it.
      utils.kill_sub_process(self.mysqlctld_process, soft=True)
      return self.mysqlctld_process
    if utils.options.keep_logs:
      return self.shutdown_mysql(extra_args=extra_args)
    return self.mysqlctl(['teardown', '-force'], extra_args=extra_args)
    
  def remove_tree(self, ignore_options=False):
    if not ignore_options and utils.options.keep_logs:
      return
    try:
      shutil.rmtree(self.tablet_dir)
    except OSError as e:
      if utils.options.verbose == 2:
        print >> sys.stderr, e, self.tablet_dir
