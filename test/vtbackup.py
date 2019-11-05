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
import sys
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
                 mysql_port=None):
        self.tablet_uid = tablet_uid or (Vtbackup.default_uid + Vtbackup.seq)
        self.mysql_port = mysql_port or (environment.reserve_ports(1))
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
        self.dbname = None
        # default to false
        self.external_mysql = False
        # utility variables
        self.tablet_alias = 'test_%s-%010d' % (self.cell, self.tablet_uid)

    def __str__(self):
        return 'vtbackup: uid: %d' % (self.tablet_uid)

    def init_backup(self, keyspace, shard,
                    tablet_index=None,
                    start=False, dbname=None, parent=True, wait_for_start=True,
                    include_mysql_port=True, **kwargs):
        """Initialize a vtbackup."""

        self.keyspace = keyspace
        self.shard = shard
        self.tablet_index = tablet_index

        self.dbname = dbname or ('vt_' + self.keyspace)

        if start:
            self.start_vtbackup(**kwargs)

    @property
    def tablet_dir(self):
        return '%s/vt_%010d' % (environment.vtdataroot, self.tablet_uid)

    def start_vtbackup(self,
                       full_mycnf_args=False,
                       extra_args=None, extra_env=None, include_mysql_port=True,
                       init_keyspace=None, init_shard=None,
                       tablet_index=None,
                       init_db=None,
                       init_db_name_override=None,
                       initial_backup=False):
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
      args.extend(environment.topo_server().flags())
      args.extend(['-pid_file', os.path.join(self.tablet_dir, 'vtbackup.pid')])
      # always enable_replication_reporter with somewhat short values for tests

      # init_db_sql_file is required to run vtbackup
      args.extend([
          "-mysql_port",str(self.mysql_port),
          "-init_db_sql_file",init_db
      ])

      # Pass through initial_backup flag to vtbackup                  
      if initial_backup:
        args.extend(["-initial_backup"])
      
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
                  
    def remove_tree(self, ignore_options=False):
      if not ignore_options and utils.options.keep_logs:
        return
      try:
        shutil.rmtree(self.tablet_dir)
      except OSError as e:
        if utils.options.verbose == 2:
          print >> sys.stderr, e, self.tablet_dir
