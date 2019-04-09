#!/usr/bin/env python
#
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

"""This test simulates the first time a database has to be split 
in a multi-vttablet-single-mysql environment

We have 2 keyspaces. One keyspace is in managing mode. It's vttablets
own the MySQL instances and can reparent, start/stop server, start/stop
replication etc. Other keyspace is in non-managing mode and cannot do
any of these actions. Only TabletExternallyReparented is allowed, but
resharding should still work.

For each keyspace:
- we start with a keyspace with a single shard and a single table
- we add and populate the sharding key
- we set the sharding key in the topology
- we clone into 2 instances
- we enable filtered replication
- we move all serving types
- we remove the source tablets
- we remove the original shard
"""

import json
import logging
import unittest
from vtdb import keyrange_constants

import MySQLdb

import base_sharding
import environment
import tablet
import utils
 
# initial shard, covers everything
ks1_shard_master = tablet.Tablet(vt_dba_passwd='VtDbaPass')
ks1_shard_replica = tablet.Tablet(vt_dba_passwd='VtDbaPass')
ks1_shard_rdonly1 = tablet.Tablet(vt_dba_passwd='VtDbaPass')

# split shards
# range '' - 80
ks1_shard_0_master = tablet.Tablet(vt_dba_passwd='VtDbaPass')
ks1_shard_0_replica = tablet.Tablet(vt_dba_passwd='VtDbaPass')
ks1_shard_0_rdonly1 = tablet.Tablet(vt_dba_passwd='VtDbaPass')
# range 80 - ''
ks1_shard_1_master = tablet.Tablet(vt_dba_passwd='VtDbaPass')
ks1_shard_1_replica = tablet.Tablet(vt_dba_passwd='VtDbaPass')
ks1_shard_1_rdonly1 = tablet.Tablet(vt_dba_passwd='VtDbaPass')

ks1_tablets = {
  '0': {'master':ks1_shard_master, 'replica':ks1_shard_replica, 'rdonly':ks1_shard_rdonly1},
  '-80': {'master':ks1_shard_0_master, 'replica':ks1_shard_0_replica, 'rdonly':ks1_shard_0_rdonly1},
  '80-': {'master':ks1_shard_1_master, 'replica':ks1_shard_1_replica, 'rdonly':ks1_shard_1_rdonly1}
}

# initial shard, covers everything
ks2_shard_master = tablet.Tablet(vt_dba_passwd='VtDbaPass')
ks2_shard_replica = tablet.Tablet(vt_dba_passwd='VtDbaPass')
ks2_shard_rdonly1 = tablet.Tablet(vt_dba_passwd='VtDbaPass')

# split shards
# range '' - 80
ks2_shard_0_master = tablet.Tablet(vt_dba_passwd='VtDbaPass')
ks2_shard_0_replica = tablet.Tablet(vt_dba_passwd='VtDbaPass')
ks2_shard_0_rdonly1 = tablet.Tablet(vt_dba_passwd='VtDbaPass')
# range 80 - ''
ks2_shard_1_master = tablet.Tablet(vt_dba_passwd='VtDbaPass')
ks2_shard_1_replica = tablet.Tablet(vt_dba_passwd='VtDbaPass')
ks2_shard_1_rdonly1 = tablet.Tablet(vt_dba_passwd='VtDbaPass')

ks2_tablets = {
  '0': {'master':ks2_shard_master, 'replica':ks2_shard_replica, 'rdonly':ks2_shard_rdonly1},
  '-80': {'master':ks2_shard_0_master, 'replica':ks2_shard_0_replica, 'rdonly':ks2_shard_0_rdonly1},
  '80-': {'master':ks2_shard_1_master, 'replica':ks2_shard_1_replica, 'rdonly':ks2_shard_1_rdonly1}
}

all_mysql_tablets = [ks1_shard_master, ks1_shard_replica, ks1_shard_rdonly1,
               ks1_shard_0_master, ks1_shard_0_replica, ks1_shard_0_rdonly1,
               ks1_shard_1_master, ks1_shard_1_replica, ks1_shard_1_rdonly1]

all_other_tablets = [ks2_shard_master, ks2_shard_replica, ks2_shard_rdonly1,
               ks2_shard_0_master, ks2_shard_0_replica, ks2_shard_0_rdonly1,
               ks2_shard_1_master, ks2_shard_1_replica, ks2_shard_1_rdonly1]

def setUpModule():
  global new_init_db, db_credentials_file

  try:
    # Determine which column is used for user passwords in this MySQL version.
    proc = ks1_shard_master.init_mysql()
    utils.wait_procs([proc])
    try:
      ks1_shard_master.mquery('mysql', 'select password from mysql.user limit 0',
                           user='root')
      password_col = 'password'
    except MySQLdb.DatabaseError:
      password_col = 'authentication_string'
    utils.wait_procs([ks1_shard_master.teardown_mysql()])
    ks1_shard_master.remove_tree(ignore_options=True)

    # Create a new init_db.sql file that sets up passwords for all users.
    # Then we use a db-credentials-file with the passwords.
    new_init_db = environment.tmproot + '/init_db_with_passwords.sql'
    with open(environment.vttop + '/config/init_db.sql') as fd:
      init_db = fd.read()
    with open(new_init_db, 'w') as fd:
      fd.write(init_db)
      fd.write('''
# Set real passwords for all users.
# connecting through a port requires 127.0.0.1
# --host=localhost will connect through socket
UPDATE mysql.user SET %s = PASSWORD('RootPass')
  WHERE User = 'root' AND Host = 'localhost';
UPDATE mysql.user SET %s = PASSWORD('VtDbaPass')
  WHERE User = 'vt_dba' AND Host = 'localhost';
UPDATE mysql.user SET %s = PASSWORD('VtAppPass')
  WHERE User = 'vt_app' AND Host = 'localhost';
UPDATE mysql.user SET %s = PASSWORD('VtAllprivsPass')
  WHERE User = 'vt_allprivs' AND Host = 'localhost';
UPDATE mysql.user SET %s = PASSWORD('VtReplPass')
  WHERE User = 'vt_repl' AND Host = '%%';
UPDATE mysql.user SET %s = PASSWORD('VtFilteredPass')
  WHERE User = 'vt_filtered' AND Host = 'localhost';

CREATE USER 'vt_dba'@'127.0.0.1' IDENTIFIED BY 'VtDbaPass';
GRANT ALL ON *.* TO 'vt_dba'@'127.0.0.1';
GRANT GRANT OPTION ON *.* TO 'vt_dba'@'127.0.0.1';

# User for app traffic, with global read-write access.
CREATE USER 'vt_app'@'127.0.0.1' IDENTIFIED BY 'VtAppPass';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,
  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_app'@'127.0.0.1';

# User for administrative operations that need to be executed as non-SUPER.
# Same permissions as vt_app here.
CREATE USER 'vt_allprivs'@'127.0.0.1' IDENTIFIED BY 'VtAllPrivsPass';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,
  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_allprivs'@'127.0.0.1';

# User for Vitess filtered replication (binlog player).
# Same permissions as vt_app.
CREATE USER 'vt_filtered'@'127.0.0.1' IDENTIFIED BY 'VtFilteredPass';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,
  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_filtered'@'127.0.0.1';

FLUSH PRIVILEGES;
''' % tuple([password_col] * 6))
    credentials = {
        'vt_dba': ['VtDbaPass'],
        'vt_app': ['VtAppPass'],
        'vt_allprivs': ['VtAllprivsPass'],
        'vt_repl': ['VtReplPass'],
        'vt_filtered': ['VtFilteredPass'],
    }
    db_credentials_file = environment.tmproot+'/db_credentials.json'
    with open(db_credentials_file, 'w') as fd:
      fd.write(json.dumps(credentials))

    setup_procs = [t.init_mysql(use_rbr=True, init_db=new_init_db,
                                 extra_args=['-db-credentials-file',
                                             db_credentials_file]) for t in all_mysql_tablets]
    utils.wait_procs(setup_procs)
    for i in range(0, len(all_other_tablets)):
      all_other_tablets[i].mysql_port = all_mysql_tablets[i].mysql_port

    environment.topo_server().setup()

  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return

  teardown_procs = [t.teardown_mysql(extra_args=['-db-credentials-file', db_credentials_file]) for t in all_mysql_tablets]
  utils.wait_procs(teardown_procs, raise_on_error=False)
  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()
  for t in all_mysql_tablets:
    t.remove_tree()
  for t in all_other_tablets:
    t.remove_tree()


class TestInitialSharding(unittest.TestCase, base_sharding.BaseShardingTest):

  # create_schema will create the same schema on the keyspace
  def _create_schema(self, keyspace):
    # Note that the primary key columns are not defined first on purpose to test
    # that a reordered column list is correctly used everywhere in vtworker.
    create_table_template = '''create table %s(
msg varchar(64),
id bigint not null,
parent_id bigint not null,
primary key (parent_id, id),
index by_msg (msg)
) Engine=InnoDB'''

    utils.run_vtctl(['ApplySchema',
                     '-sql=' + create_table_template % ('resharding1'),
                     keyspace],
                    auto_log=True)

  def _add_sharding_key_to_schema(self, keyspace):
    if base_sharding.keyspace_id_type == keyrange_constants.KIT_BYTES:
      t = 'varbinary(64)'
    else:
      t = 'bigint(20) unsigned'
    sql = 'alter table %s add custom_ksid_col ' + t
    utils.run_vtctl(['ApplySchema',
                     '-sql=' + sql % ('resharding1'),
                     keyspace],
                    auto_log=True)

  def _mark_sharding_key_not_null(self, keyspace):
    if base_sharding.keyspace_id_type == keyrange_constants.KIT_BYTES:
      t = 'varbinary(64)'
    else:
      t = 'bigint(20) unsigned'
    sql = 'alter table %s modify custom_ksid_col ' + t + ' not null'
    utils.run_vtctl(['ApplySchema',
                     '-sql=' + sql % ('resharding1'),
                     keyspace],
                    auto_log=True)

  # _insert_startup_value inserts a value in the MySQL database before it
  # is sharded
  def _insert_startup_value(self, keyspace, tablet_obj, table, mid, msg):
    tablet_obj.mquery('vt_' + keyspace, [
        'begin',
        'insert into %s(parent_id, id, msg) values(%d, %d, "%s")' %
        (table, base_sharding.fixed_parent_id, mid, msg),
        'commit'
        ], write=True)

  def _insert_startup_values(self, keyspace, master_tablet):
    self._insert_startup_value(keyspace, master_tablet, 'resharding1', 1, 'msg1')
    self._insert_startup_value(keyspace, master_tablet, 'resharding1', 2, 'msg2')
    self._insert_startup_value(keyspace, master_tablet, 'resharding1', 3, 'msg3')

  def _backfill_keyspace_id(self, keyspace, tablet_obj):
    tablet_obj.mquery('vt_' + keyspace, [
        'begin',
        'update resharding1 set custom_ksid_col=0x1000000000000000 where id=1',
        'update resharding1 set custom_ksid_col=0x9000000000000000 where id=2',
        'update resharding1 set custom_ksid_col=0xD000000000000000 where id=3',
        'commit'
        ], write=True)

  def _check_startup_values(self, keyspace, tablets):
    # check first value is in the left shard
    for t in tablets['-80'].values():
      self._check_value(t, 'resharding1', 1, 'msg1', 0x1000000000000000)
    for t in tablets['80-'].values():
      self._check_value(t, 'resharding1', 1, 'msg1',
                        0x1000000000000000, should_be_here=False)

    # check second value is in the right shard
    for t in tablets['-80'].values():
      self._check_value(t, 'resharding1', 2, 'msg2', 0x9000000000000000,
                        should_be_here=False)
    for t in tablets['80-'].values():
      self._check_value(t, 'resharding1', 2, 'msg2', 0x9000000000000000)

    # check third value is in the right shard too
    for t in tablets['-80'].values():
      self._check_value(t, 'resharding1', 3, 'msg3', 0xD000000000000000,
                        should_be_here=False)
    for t in tablets['80-'].values():
      self._check_value(t, 'resharding1', 3, 'msg3', 0xD000000000000000)

  def _insert_lots(self, keyspace, master_tablet, count, base=0):
    for i in xrange(count):
      self._insert_value(master_tablet, 'resharding1', 10000 + base + i,
                         'msg-range1-%d' % i, 0xA000000000000000 + base + i)
      self._insert_value(master_tablet, 'resharding1', 20000 + base + i,
                         'msg-range2-%d' % i, 0xE000000000000000 + base + i)

  # _check_lots returns how many of the values we have, in percents.
  def _check_lots(self, replica_tablet, count, base=0):
    found = 0
    for i in xrange(count):
      if self._is_value_present_and_correct(replica_tablet, 'resharding1',
                                            10000 + base + i, 'msg-range1-%d' %
                                            i, 0xA000000000000000 + base + i):
        found += 1
      if self._is_value_present_and_correct(replica_tablet, 'resharding1',
                                            20000 + base + i, 'msg-range2-%d' %
                                            i, 0xE000000000000000 + base + i):
        found += 1
    percent = found * 100 / count / 2
    logging.debug('I have %d%% of the data', percent)
    return percent

  def _check_lots_timeout(self, replica_tablet, count, threshold, timeout, base=0):
    while True:
      value = self._check_lots(replica_tablet, count, base=base)
      if value >= threshold:
        return value
      timeout = utils.wait_step('enough data went through', timeout)

  # _check_lots_not_present makes sure no data is in the wrong shard
  def _check_lots_not_present(self, replica_tablet, count, base=0):
    for i in xrange(count):
      self._check_value(replica_tablet, 'resharding1', 10000 + base + i,
                        'msg-range1-%d' % i, 0xA000000000000000 + base + i,
                        should_be_here=False)
      self._check_value(replica_tablet, 'resharding1', 20000 + base + i,
                        'msg-range2-%d' % i, 0xE000000000000000 + base + i,
                        should_be_here=False)

  def _test_resharding(self, keyspace, tablet_map, external_mysql=False):
    # create the keyspace with just one shard
    shard_master = tablet_map['0']['master']
    shard_replica = tablet_map['0']['replica']
    shard_rdonly = tablet_map['0']['rdonly']
    shard_0_master = tablet_map['-80']['master']
    shard_0_replica = tablet_map['-80']['replica']
    shard_0_rdonly = tablet_map['-80']['rdonly']
    shard_1_master = tablet_map['80-']['master']
    shard_1_replica = tablet_map['80-']['replica']
    shard_1_rdonly = tablet_map['80-']['rdonly']
    shard_master.init_tablet(
        'replica',
        keyspace=keyspace,
        shard='0',
        tablet_index=0,
        external_mysql=external_mysql,
        extra_args=['-db-credentials-file', db_credentials_file])
    shard_replica.init_tablet(
        'replica',
        keyspace=keyspace,
        shard='0',
        tablet_index=1,
        external_mysql=external_mysql,
        extra_args=['-db-credentials-file', db_credentials_file])
    shard_rdonly.init_tablet(
        'rdonly',
        keyspace=keyspace,
        shard='0',
        tablet_index=2,
        external_mysql=external_mysql,
        extra_args=['-db-credentials-file', db_credentials_file])

    for t in [shard_master, shard_replica, shard_rdonly]:
      t.create_db('vt_' + keyspace)

    shard_master.start_vttablet(wait_for_state=None,
                                binlog_use_v3_resharding_mode=False,
                                supports_backups=False,
                       extra_args=['-db-credentials-file', db_credentials_file])
    shard_rdonly.start_vttablet(wait_for_state=None,
                                binlog_use_v3_resharding_mode=False,
                                supports_backups=False,
                       extra_args=['-db-credentials-file', db_credentials_file])

    if not external_mysql:
      for t in [shard_master, shard_rdonly]:
        t.wait_for_vttablet_state('NOT_SERVING')

    # start replica
    shard_replica.start_vttablet(wait_for_state=None,
                                binlog_use_v3_resharding_mode=False,
                                supports_backups=False,
                                extra_args=['-db-credentials-file', db_credentials_file])

    if not external_mysql:
      shard_replica.wait_for_vttablet_state('NOT_SERVING')

      # reparent to make the tablets work
      utils.run_vtctl(['InitShardMaster', '-force', keyspace+'/0',
                       shard_master.tablet_alias], auto_log=True)

      utils.wait_for_tablet_type(shard_replica.tablet_alias, 'replica')
      utils.wait_for_tablet_type(shard_rdonly.tablet_alias, 'rdonly')
    else:
      shard_replica.wait_for_vttablet_state('SERVING')
      # default mode is VTCTL_AUTO which makes this command hang
      _, stderr = utils.run_vtctl(['TabletExternallyReparented', shard_master.tablet_alias], mode=utils.VTCTL_VTCTL, auto_log=True)
        
    for t in [shard_master, shard_replica, shard_rdonly]:
      t.wait_for_vttablet_state('SERVING')

    # create the tables and add startup values
    self._create_schema(keyspace)
    self._insert_startup_values(keyspace, shard_master)

    # reload schema on all tablets so we can query them
    for t in [shard_master, shard_replica, shard_rdonly]:
      utils.run_vtctl(['ReloadSchema', t.tablet_alias], auto_log=True)

    # We must start vtgate after tablets are up, or else wait until 1min refresh
    # (that is the tablet_refresh_interval parameter for discovery gateway)
    # we want cache_ttl at zero so we re-read the topology for every test query.

    utils.VtGate().start(cache_ttl='0', tablets=[
        shard_master, shard_replica, shard_rdonly])
    utils.vtgate.wait_for_endpoints(keyspace + '.0.master', 1)
    utils.vtgate.wait_for_endpoints(keyspace + '.0.replica', 1)
    utils.vtgate.wait_for_endpoints(keyspace + '.0.rdonly', 1)

    # check the Map Reduce API works correctly, should use ExecuteShards,
    # as we're not sharded yet.
    # we have 3 values in the database, asking for 4 splits will get us
    # a single query.
    sql = 'select id, msg from resharding1'
    s = utils.vtgate.split_query(sql, keyspace, 4)
    self.assertEqual(len(s), 1)
    self.assertEqual(s[0]['shard_part']['shards'][0], '0')

    # change the schema, backfill keyspace_id, and change schema again
    self._add_sharding_key_to_schema(keyspace)
    self._backfill_keyspace_id(keyspace, shard_master)
    self._mark_sharding_key_not_null(keyspace)

    # now we can be a sharded keyspace (and propagate to SrvKeyspace)
    utils.run_vtctl(['SetKeyspaceShardingInfo', keyspace,
                     'custom_ksid_col', base_sharding.keyspace_id_type])
    utils.run_vtctl(['RebuildKeyspaceGraph', keyspace],
                    auto_log=True)

    # run a health check on source replica so it responds to discovery
    utils.run_vtctl(['RunHealthCheck', shard_replica.tablet_alias])

    # create the split shards
    shard_0_master.init_tablet(
        'replica',
        keyspace=keyspace,
        shard='-80',
        tablet_index=0,
        external_mysql=external_mysql,
        extra_args=['-db-credentials-file', db_credentials_file])
    shard_0_replica.init_tablet(
        'replica',
        keyspace=keyspace,
        shard='-80',
        tablet_index=1,
        external_mysql=external_mysql,
        extra_args=['-db-credentials-file', db_credentials_file])
    shard_0_rdonly.init_tablet(
        'rdonly',
        keyspace=keyspace,
        shard='-80',
        tablet_index=2,
        external_mysql=external_mysql,
        extra_args=['-db-credentials-file', db_credentials_file])
    shard_1_master.init_tablet(
        'replica',
        keyspace=keyspace,
        shard='80-',
        tablet_index=0,
        external_mysql=external_mysql,
        extra_args=['-db-credentials-file', db_credentials_file])
    shard_1_replica.init_tablet(
        'replica',
        keyspace=keyspace,
        shard='80-',
        tablet_index=1,
        external_mysql=external_mysql,
        extra_args=['-db-credentials-file', db_credentials_file])
    shard_1_rdonly.init_tablet(
        'rdonly',
        keyspace=keyspace,
        shard='80-',
        tablet_index=2,
        external_mysql=external_mysql,
        extra_args=['-db-credentials-file', db_credentials_file])

    for t in [shard_0_master, shard_0_replica, shard_0_rdonly,
              shard_1_master, shard_1_replica, shard_1_rdonly]:
      t.create_db('vt_' + keyspace)
      t.start_vttablet(wait_for_state=None,
                       binlog_use_v3_resharding_mode=False,
                       supports_backups=False,
                       extra_args=['-db-credentials-file', db_credentials_file])

    sharded_tablets = [shard_0_master, shard_0_replica, shard_0_rdonly,
                       shard_1_master, shard_1_replica, shard_1_rdonly]
    if not external_mysql:
      for t in sharded_tablets:
        t.wait_for_vttablet_state('NOT_SERVING')

      utils.run_vtctl(['InitShardMaster', '-force', keyspace + '/-80',
                       shard_0_master.tablet_alias], auto_log=True)
      utils.run_vtctl(['InitShardMaster', '-force', keyspace + '/80-',
                     shard_1_master.tablet_alias], auto_log=True)

      for t in [shard_0_replica, shard_1_replica]:
        utils.wait_for_tablet_type(t.tablet_alias, 'replica')
      for t in [shard_0_rdonly, shard_1_rdonly]:
        utils.wait_for_tablet_type(t.tablet_alias, 'rdonly')

      for t in sharded_tablets:
        t.wait_for_vttablet_state('SERVING')
    else:
      # default mode is VTCTL_AUTO which makes this command hang
      _, stderr = utils.run_vtctl(['TabletExternallyReparented', shard_0_master.tablet_alias], mode=utils.VTCTL_VTCTL, auto_log=True)
      _, stderr = utils.run_vtctl(['TabletExternallyReparented', shard_1_master.tablet_alias], mode=utils.VTCTL_VTCTL, auto_log=True)

    # must restart vtgate after tablets are up, or else wait until 1min refresh
    # we want cache_ttl at zero so we re-read the topology for every test query.
    utils.vtgate.kill()

    utils.vtgate = None
    utils.VtGate().start(cache_ttl='0', tablets=[
        shard_master, shard_replica, shard_rdonly,
        shard_0_master, shard_0_replica, shard_0_rdonly,
        shard_1_master, shard_1_replica, shard_1_rdonly])
    var = None

    # Wait for the endpoints, either local or remote.
    utils.vtgate.wait_for_endpoints(keyspace + '.0.master', 1, var=var)
    utils.vtgate.wait_for_endpoints(keyspace + '.0.replica', 1, var=var)
    utils.vtgate.wait_for_endpoints(keyspace + '.0.rdonly', 1, var=var)
    utils.vtgate.wait_for_endpoints(keyspace + '.-80.master', 1, var=var)
    utils.vtgate.wait_for_endpoints(keyspace + '.-80.replica', 1, var=var)
    utils.vtgate.wait_for_endpoints(keyspace + '.-80.rdonly', 1, var=var)
    utils.vtgate.wait_for_endpoints(keyspace + '.80-.master', 1, var=var)
    utils.vtgate.wait_for_endpoints(keyspace + '.80-.replica', 1, var=var)
    utils.vtgate.wait_for_endpoints(keyspace + '.80-.rdonly', 1, var=var)

    # check the Map Reduce API works correctly, should use ExecuteKeyRanges now,
    # as we are sharded (with just one shard).
    # again, we have 3 values in the database, asking for 4 splits will get us
    # a single query.
    sql = 'select id, msg from resharding1'
    s = utils.vtgate.split_query(sql, keyspace, 4)
    self.assertEqual(len(s), 1)
    self.assertEqual(s[0]['key_range_part']['keyspace'], keyspace)
    # There must be one empty KeyRange which represents the full keyspace.
    self.assertEqual(len(s[0]['key_range_part']['key_ranges']), 1)
    self.assertEqual(s[0]['key_range_part']['key_ranges'][0], {})

    utils.check_srv_keyspace('test_nj', keyspace,
                             'Partitions(master): -\n'
                             'Partitions(rdonly): -\n'
                             'Partitions(replica): -\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    # we need to create the schema, and the worker will do data copying
    for keyspace_shard in (keyspace + '/-80', keyspace + '/80-'):
      utils.run_vtctl(['CopySchemaShard',
                       '--exclude_tables', 'unrelated',
                       shard_rdonly.tablet_alias,
                       keyspace_shard],
                      auto_log=True)
    utils.run_vtctl(['RunHealthCheck', shard_rdonly.tablet_alias])

    # Run vtworker as daemon for the following SplitClone commands.
    worker_proc, worker_port, worker_rpc_port = utils.run_vtworker_bg(
        ['--cell', 'test_nj', '--command_display_interval', '10ms',
         '--use_v3_resharding_mode=false'],
        auto_log=True)

    # Initial clone (online).
    workerclient_proc = utils.run_vtworker_client_bg(
        ['SplitClone',
         '--offline=false',
         '--exclude_tables', 'unrelated',
         '--chunk_count', '10',
         '--min_rows_per_chunk', '1',
         '--min_healthy_rdonly_tablets', '1',
         keyspace + '/0'],
        worker_rpc_port)
    utils.wait_procs([workerclient_proc])
    self.verify_reconciliation_counters(worker_port, 'Online', 'resharding1',
                                        3, 0, 0, 0)

    # Reset vtworker such that we can run the next command.
    workerclient_proc = utils.run_vtworker_client_bg(['Reset'], worker_rpc_port)
    utils.wait_procs([workerclient_proc])

    # Modify the destination shard. SplitClone will revert the changes.
    # Delete row 1 (provokes an insert).
    shard_0_master.mquery('vt_' + keyspace,
                          'delete from resharding1 where id=1', write=True)
    # Delete row 2 (provokes an insert).
    shard_1_master.mquery('vt_' + keyspace,
                          'delete from resharding1 where id=2', write=True)
    # Update row 3 (provokes an update).
    shard_1_master.mquery('vt_' + keyspace,
                          "update resharding1 set msg='msg-not-3' where id=3",
                          write=True)
    # Insert row 4 (provokes a delete).
    self._insert_value(shard_1_master, 'resharding1', 4, 'msg4',
                       0xD000000000000000)

    workerclient_proc = utils.run_vtworker_client_bg(
        ['SplitClone',
         '--exclude_tables', 'unrelated',
         '--chunk_count', '10',
         '--min_rows_per_chunk', '1',
         '--min_healthy_rdonly_tablets', '1',
         keyspace + '/0'],
        worker_rpc_port)
    utils.wait_procs([workerclient_proc])
    self.verify_reconciliation_counters(worker_port, 'Online', 'resharding1',
                                        2, 1, 1, 0)
    self.verify_reconciliation_counters(worker_port, 'Offline', 'resharding1',
                                        0, 0, 0, 3)
    # Terminate worker daemon because it is no longer needed.
    utils.kill_sub_process(worker_proc, soft=True)

    # check the startup values are in the right place
    self._check_startup_values(keyspace, tablet_map)

    # check the schema too
    utils.run_vtctl(['ValidateSchemaKeyspace', keyspace], auto_log=True)

    # check the binlog players are running
    logging.debug('Waiting for binlog players to start on new masters...')
    self.check_destination_master(shard_0_master, [keyspace + '/0'])
    self.check_destination_master(shard_1_master, [keyspace + '/0'])

    # check that binlog server exported the stats vars
    self.check_binlog_server_vars(shard_replica, horizontal=True)

    # testing filtered replication: insert a bunch of data on shard 1,
    # check we get most of it after a few seconds, wait for binlog server
    # timeout, check we get all of it.
    logging.debug('Inserting lots of data on source shard')
    self._insert_lots(keyspace, shard_master, 1000)
    logging.debug('Checking 80 percent of data is sent quickly')
    v = self._check_lots_timeout(shard_1_replica, 1000, 80, 5)
    if v != 100:
      logging.debug('Checking all data goes through eventually')
      self._check_lots_timeout(shard_1_replica, 1000, 100, 20)
    logging.debug('Checking no data was sent the wrong way')
    self._check_lots_not_present(shard_0_replica, 1000)
    self.check_binlog_player_vars(shard_0_master, [keyspace + '/0'],
                                  seconds_behind_master_max=30)
    self.check_binlog_player_vars(shard_1_master, [keyspace + '/0'],
                                  seconds_behind_master_max=30)
    self.check_binlog_server_vars(shard_replica, horizontal=True,
                                  min_statements=1000, min_transactions=1000)

    # use vtworker to compare the data
    for t in [shard_0_rdonly, shard_1_rdonly]:
      utils.run_vtctl(['RunHealthCheck', t.tablet_alias])

    # get status for the destination master tablet, make sure we have it all
    if not external_mysql:
      self.check_running_binlog_player(shard_0_master, 2000, 2000)
      self.check_running_binlog_player(shard_1_master, 6000, 2000)
    else:
      self.check_running_binlog_player(shard_0_master, 2002, 2002)
      self.check_running_binlog_player(shard_1_master, 6002, 2002)
      

    # check we can't migrate the master just yet
    utils.run_vtctl(['MigrateServedTypes', keyspace + '/0', 'master'],
                    expect_fail=True)

    # now serve rdonly from the split shards
    utils.run_vtctl(['MigrateServedTypes', keyspace + '/0', 'rdonly'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', keyspace,
                             'Partitions(master): -\n'
                             'Partitions(rdonly): -80 80-\n'
                             'Partitions(replica): -\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    # make sure rdonly tablets are back to serving before hitting vtgate.
    for t in [shard_0_rdonly, shard_1_rdonly]:
      t.wait_for_vttablet_state('SERVING')

    utils.vtgate.wait_for_endpoints(keyspace + '.-80.rdonly', 1)
    utils.vtgate.wait_for_endpoints(keyspace + '.80-.rdonly', 1)

    # check the Map Reduce API works correctly, should use ExecuteKeyRanges
    # on both destination shards now.
    # we ask for 2 splits to only have one per shard
    sql = 'select id, msg from resharding1'
    timeout = 10.0
    while True:
      try:
        s = utils.vtgate.split_query(sql, keyspace, 2)
        break
      except Exception:  # pylint: disable=broad-except
        timeout = utils.wait_step(
            'vtgate executes split_query properly', timeout)
    self.assertEqual(len(s), 2)
    self.assertEqual(s[0]['key_range_part']['keyspace'], keyspace)
    self.assertEqual(s[1]['key_range_part']['keyspace'], keyspace)
    self.assertEqual(len(s[0]['key_range_part']['key_ranges']), 1)
    self.assertEqual(len(s[1]['key_range_part']['key_ranges']), 1)

    # then serve replica from the split shards
    source_tablet = shard_replica
    destination_tablets = [shard_0_replica, shard_1_replica]

    utils.run_vtctl(
        ['MigrateServedTypes', keyspace + '/0', 'replica'], auto_log=True)
    utils.check_srv_keyspace('test_nj', keyspace,
                             'Partitions(master): -\n'
                             'Partitions(rdonly): -80 80-\n'
                             'Partitions(replica): -80 80-\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    # move replica back and forth
    utils.run_vtctl(
        ['MigrateServedTypes', '-reverse', keyspace + '/0', 'replica'],
        auto_log=True)
    # After a backwards migration, queryservice should be enabled on
    # source and disabled on destinations
    utils.check_tablet_query_service(self, source_tablet, True, False)
    utils.check_tablet_query_services(self, destination_tablets, False, True)
    utils.check_srv_keyspace('test_nj', keyspace,
                             'Partitions(master): -\n'
                             'Partitions(rdonly): -80 80-\n'
                             'Partitions(replica): -\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    utils.run_vtctl(['MigrateServedTypes', keyspace + '/0', 'replica'],
                    auto_log=True)
    # After a forwards migration, queryservice should be disabled on
    # source and enabled on destinations
    utils.check_tablet_query_service(self, source_tablet, False, True)
    utils.check_tablet_query_services(self, destination_tablets, True, False)
    utils.check_srv_keyspace('test_nj', keyspace,
                             'Partitions(master): -\n'
                             'Partitions(rdonly): -80 80-\n'
                             'Partitions(replica): -80 80-\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    # then serve master from the split shards
    utils.run_vtctl(['MigrateServedTypes', keyspace + '/0', 'master'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', keyspace,
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-\n'
                             'Partitions(replica): -80 80-\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    # check the binlog players are gone now
    self.check_no_binlog_player(shard_0_master)
    self.check_no_binlog_player(shard_1_master)

  def kill_all_tablets(self, keyspace, tablet_map):
    shard_master = tablet_map['0']['master']
    shard_replica = tablet_map['0']['replica']
    shard_rdonly = tablet_map['0']['rdonly']
    shard_0_master = tablet_map['-80']['master']
    shard_0_replica = tablet_map['-80']['replica']
    shard_0_rdonly = tablet_map['-80']['rdonly']
    shard_1_master = tablet_map['80-']['master']
    shard_1_replica = tablet_map['80-']['replica']
    shard_1_rdonly = tablet_map['80-']['rdonly']

    # remove the original tablets in the original shard
    tablet.kill_tablets([shard_master, shard_replica, shard_rdonly])
    for t in [shard_replica, shard_rdonly]:
      utils.run_vtctl(['DeleteTablet', t.tablet_alias], auto_log=True)
    utils.run_vtctl(['DeleteTablet', '-allow_master',
                     shard_master.tablet_alias], auto_log=True)

    # rebuild the serving graph, all mentions of the old shards should be gone
    utils.run_vtctl(['RebuildKeyspaceGraph', keyspace], auto_log=True)

    # delete the original shard
    utils.run_vtctl(['DeleteShard', keyspace + '/0'], auto_log=True)

    # kill everything else
    tablet.kill_tablets([shard_0_master, shard_0_replica, shard_0_rdonly,
                         shard_1_master, shard_1_replica, shard_1_rdonly])

  def test_resharding(self):
    self._test_resharding('test_keyspace1', ks1_tablets)
    self._test_resharding('test_keyspace2', ks2_tablets, True)
    self.kill_all_tablets('test_keyspace1', ks1_tablets)
    self.kill_all_tablets('test_keyspace2', ks2_tablets)

if __name__ == '__main__':
  utils.main()
