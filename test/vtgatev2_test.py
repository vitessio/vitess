#!/usr/bin/env python
# coding: utf-8

import logging
from multiprocessing.pool import ThreadPool
import pprint
import struct
import threading
import time
import traceback
import unittest

import environment
import tablet
import utils
from vtdb import dbexceptions
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import vtdb_logger
from vtdb import vtgate_client
from vtdb import vtgate_cursor

# use_l2vtgate controls if we're adding a l2vtgate process in between
# vtgate and the tablets.
use_l2vtgate = False

# l2vtgate is the L2VTGate object, if any
l2vtgate = None

# l2vtgate_param is the parameter to send to vtgate
l2vtgate_param = None

shard_0_master = tablet.Tablet()
shard_0_replica1 = tablet.Tablet()
shard_0_replica2 = tablet.Tablet()

shard_1_master = tablet.Tablet()
shard_1_replica1 = tablet.Tablet()
shard_1_replica2 = tablet.Tablet()

KEYSPACE_NAME = 'test_keyspace'
SHARD_NAMES = ['-80', '80-']
SHARD_KID_MAP = {
    '-80': [
        527875958493693904, 626750931627689502,
        345387386794260318, 332484755310826578,
        1842642426274125671, 1326307661227634652,
        1761124146422844620, 1661669973250483744,
        3361397649937244239, 2444880764308344533],
    '80-': [
        9767889778372766922, 9742070682920810358,
        10296850775085416642, 9537430901666854108,
        10440455099304929791, 11454183276974683945,
        11185910247776122031, 10460396697869122981,
        13379616110062597001, 12826553979133932576],
}

CREATE_VT_INSERT_TEST = '''create table vt_insert_test (
  id bigint auto_increment,
  msg varchar(64),
  keyspace_id bigint(20) unsigned NOT NULL,
  primary key (id)
) Engine=InnoDB'''

CREATE_VT_A = '''create table vt_a (
  eid bigint,
  id int,
  keyspace_id bigint(20) unsigned NOT NULL,
  primary key(eid, id)
) Engine=InnoDB'''

CREATE_VT_FIELD_TYPES = '''create table vt_field_types (
  id bigint(20) auto_increment,
  uint_val bigint(20) unsigned,
  str_val varchar(64),
  unicode_val varchar(64),
  float_val float(5, 1),
  keyspace_id bigint(20) unsigned NOT NULL,
  primary key(id)
) Engine=InnoDB'''

CREATE_VT_SEQ = '''create table vt_seq (
  id int,
  next_id bigint,
  cache bigint,
  primary key(id)
) comment 'vitess_sequence' Engine=InnoDB'''

INIT_VT_SEQ = 'insert into vt_seq values(0, 1, 2)'


create_tables = [
    CREATE_VT_INSERT_TEST,
    CREATE_VT_A,
    CREATE_VT_FIELD_TYPES,
    CREATE_VT_SEQ,
]
pack_kid = struct.Struct('!Q').pack


class DBRow(object):

  def __init__(self, column_names, row_tuple):
    self.__dict__ = dict(zip(column_names, row_tuple))

  def __repr__(self):
    return pprint.pformat(self.__dict__, 4)


def setUpModule():
  logging.debug('in setUpModule')
  try:
    environment.topo_server().setup()

    # start mysql instance external to the test
    setup_procs = [shard_0_master.init_mysql(),
                   shard_0_replica1.init_mysql(),
                   shard_0_replica2.init_mysql(),
                   shard_1_master.init_mysql(),
                   shard_1_replica1.init_mysql(),
                   shard_1_replica2.init_mysql()
                  ]
    utils.wait_procs(setup_procs)
    setup_tablets()
  except Exception, e:  # pylint: disable=broad-except
    logging.exception('error during set up: %s', e)
    tearDownModule()
    raise


def tearDownModule():
  logging.debug('in tearDownModule')
  utils.required_teardown()
  if utils.options.skip_teardown:
    return
  logging.debug('Tearing down the servers and setup')
  if utils.vtgate:
    utils.vtgate.kill()
  if l2vtgate:
    l2vtgate.kill()
  tablet.kill_tablets([shard_0_master,
                       shard_0_replica1, shard_0_replica2,
                       shard_1_master,
                       shard_1_replica1, shard_1_replica2])
  teardown_procs = [shard_0_master.teardown_mysql(),
                    shard_0_replica1.teardown_mysql(),
                    shard_0_replica2.teardown_mysql(),
                    shard_1_master.teardown_mysql(),
                    shard_1_replica1.teardown_mysql(),
                    shard_1_replica2.teardown_mysql(),
                   ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()

  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_replica1.remove_tree()
  shard_0_replica2.remove_tree()
  shard_1_master.remove_tree()
  shard_1_replica1.remove_tree()
  shard_1_replica2.remove_tree()


def setup_tablets():
  """Start up a master mysql and vttablet."""
  global l2vtgate, l2vtgate_param

  logging.debug('Setting up tablets')
  utils.run_vtctl(['CreateKeyspace', KEYSPACE_NAME])
  utils.run_vtctl(['SetKeyspaceShardingInfo', '-force', KEYSPACE_NAME,
                   'keyspace_id', 'uint64'])
  shard_0_master.init_tablet(
      'replica',
      keyspace=KEYSPACE_NAME,
      shard='-80',
      tablet_index=0)
  shard_0_replica1.init_tablet(
      'replica',
      keyspace=KEYSPACE_NAME,
      shard='-80',
      tablet_index=1)
  shard_0_replica2.init_tablet(
      'replica',
      keyspace=KEYSPACE_NAME,
      shard='-80',
      tablet_index=2)
  shard_1_master.init_tablet(
      'replica',
      keyspace=KEYSPACE_NAME,
      shard='80-',
      tablet_index=0)
  shard_1_replica1.init_tablet(
      'replica',
      keyspace=KEYSPACE_NAME,
      shard='80-',
      tablet_index=1)
  shard_1_replica2.init_tablet(
      'replica',
      keyspace=KEYSPACE_NAME,
      shard='80-',
      tablet_index=2)

  utils.run_vtctl(['RebuildKeyspaceGraph', KEYSPACE_NAME], auto_log=True)

  for t in [shard_0_master, shard_0_replica1, shard_0_replica2,
            shard_1_master, shard_1_replica1, shard_1_replica2]:
    t.create_db('vt_test_keyspace')
    for create_table in create_tables:
      t.mquery(shard_0_master.dbname, create_table)
    t.start_vttablet(wait_for_state=None)

  for t in [shard_0_master, shard_0_replica1, shard_0_replica2,
            shard_1_master, shard_1_replica1, shard_1_replica2]:
    t.wait_for_vttablet_state('NOT_SERVING')

  utils.run_vtctl(['InitShardMaster', '-force', KEYSPACE_NAME+'/-80',
                   shard_0_master.tablet_alias], auto_log=True)
  utils.run_vtctl(['InitShardMaster', '-force', KEYSPACE_NAME+'/80-',
                   shard_1_master.tablet_alias], auto_log=True)

  for t in [shard_0_master, shard_0_replica1, shard_0_replica2,
            shard_1_master, shard_1_replica1, shard_1_replica2]:
    t.wait_for_vttablet_state('SERVING')

  utils.run_vtctl(
      ['RebuildKeyspaceGraph', KEYSPACE_NAME], auto_log=True)

  utils.check_srv_keyspace(
      'test_nj', KEYSPACE_NAME,
      'Partitions(master): -80 80-\n'
      'Partitions(rdonly): -80 80-\n'
      'Partitions(replica): -80 80-\n')

  if use_l2vtgate:
    l2vtgate = utils.L2VtGate()
    l2vtgate.start(tablets=
                   [shard_0_master, shard_0_replica1, shard_0_replica2,
                    shard_1_master, shard_1_replica1, shard_1_replica2])
    _, addr = l2vtgate.rpc_endpoint()
    l2vtgate_param = '%s|%s|%s' % (addr, KEYSPACE_NAME, '-')
    utils.VtGate().start(l2vtgates=[l2vtgate_param,])

  else:
    utils.VtGate().start(tablets=
                         [shard_0_master, shard_0_replica1, shard_0_replica2,
                          shard_1_master, shard_1_replica1, shard_1_replica2])

  wait_for_endpoints(
      '%s.%s.master' % (KEYSPACE_NAME, SHARD_NAMES[0]),
      1)
  wait_for_endpoints(
      '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[0]),
      2)
  wait_for_endpoints(
      '%s.%s.master' % (KEYSPACE_NAME, SHARD_NAMES[1]),
      1)
  wait_for_endpoints(
      '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[1]),
      2)


def restart_vtgate(port):
  if use_l2vtgate:
    utils.VtGate(port=port).start(l2vtgates=[l2vtgate_param,])
  else:
    utils.VtGate(port=port).start(
        tablets=[shard_0_master, shard_0_replica1, shard_0_replica2,
                 shard_1_master, shard_1_replica1, shard_1_replica2])


def wait_for_endpoints(name, count):
  if use_l2vtgate:
    l2vtgate.wait_for_endpoints(name, count)
  else:
    utils.vtgate.wait_for_endpoints(name, count)


def get_connection(timeout=10.0):
  protocol, endpoint = utils.vtgate.rpc_endpoint(python=True)
  try:
    return vtgate_client.connect(protocol, endpoint, timeout)
  except Exception:  # pylint: disable=broad-except
    logging.exception('Connection to vtgate (timeout=%s) failed.', timeout)
    raise


def _delete_all(shard_index, table_name):
  vtgate_conn = get_connection()
  # This write is to set up the test with fresh insert
  # and hence performing it directly on the connection.
  vtgate_conn.begin()
  vtgate_conn._execute(
      'delete from %s' % table_name, {},
      tablet_type='master', keyspace_name=KEYSPACE_NAME,
      keyranges=[keyrange.KeyRange(SHARD_NAMES[shard_index])])
  vtgate_conn.commit()
  vtgate_conn.close()


def write_rows_to_shard(count, shard_index):
  kid_list = SHARD_KID_MAP[SHARD_NAMES[shard_index]]
  _delete_all(shard_index, 'vt_insert_test')
  vtgate_conn = get_connection()

  for x in xrange(count):
    keyspace_id = kid_list[x % len(kid_list)]
    cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace=KEYSPACE_NAME,
        keyspace_ids=[pack_kid(keyspace_id)],
        writable=True)
    cursor.begin()
    cursor.execute(
        'insert into vt_insert_test (msg, keyspace_id) '
        'values (:msg, :keyspace_id)',
        {'msg': 'test %s' % x, 'keyspace_id': keyspace_id})
    cursor.commit()
  vtgate_conn.close()


class BaseTestCase(unittest.TestCase):

  def setUp(self):
    super(BaseTestCase, self).setUp()
    logging.info('Start: %s.', '.'.join(self.id().split('.')[-2:]))


class TestCoreVTGateFunctions(BaseTestCase):

  def setUp(self):
    super(TestCoreVTGateFunctions, self).setUp()
    self.shard_index = 1
    self.keyrange = keyrange.KeyRange(SHARD_NAMES[self.shard_index])
    self.master_tablet = shard_1_master
    self.replica_tablet = shard_1_replica1

  def test_status(self):
    self.assertIn('</html>', utils.vtgate.get_status())

  def test_connect(self):
    vtgate_conn = get_connection()
    self.assertNotEqual(vtgate_conn, None)
    vtgate_conn.close()

  def test_writes(self):
    vtgate_conn = get_connection()
    _delete_all(self.shard_index, 'vt_insert_test')
    count = 10
    kid_list = SHARD_KID_MAP[SHARD_NAMES[self.shard_index]]
    for x in xrange(count):
      keyspace_id = kid_list[count%len(kid_list)]
      cursor = vtgate_conn.cursor(
          tablet_type='master', keyspace=KEYSPACE_NAME,
          keyspace_ids=[pack_kid(keyspace_id)],
          writable=True)
      cursor.begin()
      cursor.execute(
          'insert into vt_insert_test (msg, keyspace_id) '
          'values (:msg, :keyspace_id)',
          {'msg': 'test %s' % x, 'keyspace_id': keyspace_id})
      cursor.commit()
    cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    rowcount = cursor.execute('select * from vt_insert_test', {})
    self.assertEqual(rowcount, count, 'master fetch works')
    vtgate_conn.close()

  def test_query_routing(self):
    """Test VtGate routes queries to the right tablets."""
    row_counts = [20, 30]
    for shard_index in [0, 1]:
      write_rows_to_shard(row_counts[shard_index], shard_index)
    vtgate_conn = get_connection()
    for shard_index in [0, 1]:
      # Fetch all rows in each shard
      cursor = vtgate_conn.cursor(
          tablet_type='master', keyspace=KEYSPACE_NAME,
          keyranges=[keyrange.KeyRange(SHARD_NAMES[shard_index])])
      rowcount = cursor.execute('select * from vt_insert_test', {})
      # Verify row count
      self.assertEqual(rowcount, row_counts[shard_index])
      # Verify keyspace id
      for result in cursor.results:
        kid = result[2]
        self.assertIn(kid, SHARD_KID_MAP[SHARD_NAMES[shard_index]])

    # Do a cross shard range query and assert all rows are fetched.
    # Use this test to also test the vtgate vars (and l2vtgate vars if
    # applicable) are correctly updated.
    v = utils.vtgate.get_vars()
    key0 = 'Execute.' + KEYSPACE_NAME + '.' + SHARD_NAMES[0] + '.master'
    key1 = 'Execute.' + KEYSPACE_NAME + '.' + SHARD_NAMES[1] + '.master'
    before0 = v['VttabletCall']['Histograms'][key0]['Count']
    before1 = v['VttabletCall']['Histograms'][key1]['Count']
    if use_l2vtgate:
      lv = l2vtgate.get_vars()
      lbefore0 = lv['VttabletCall']['Histograms'][key0]['Count']
      lbefore1 = lv['VttabletCall']['Histograms'][key1]['Count']

    cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace=KEYSPACE_NAME,
        keyranges=[keyrange.KeyRange('75-95')])
    rowcount = cursor.execute('select * from vt_insert_test', {})
    self.assertEqual(rowcount, row_counts[0] + row_counts[1])
    vtgate_conn.close()

    v = utils.vtgate.get_vars()
    after0 = v['VttabletCall']['Histograms'][key0]['Count']
    after1 = v['VttabletCall']['Histograms'][key1]['Count']
    self.assertEqual(after0 - before0, 1)
    self.assertEqual(after1 - before1, 1)
    if use_l2vtgate:
      lv = l2vtgate.get_vars()
      lafter0 = lv['VttabletCall']['Histograms'][key0]['Count']
      lafter1 = lv['VttabletCall']['Histograms'][key1]['Count']
      self.assertEqual(lafter0 - lbefore0, 1)
      self.assertEqual(lafter1 - lbefore1, 1)

  def test_rollback(self):
    vtgate_conn = get_connection()
    count = 10
    _delete_all(self.shard_index, 'vt_insert_test')
    kid_list = SHARD_KID_MAP[SHARD_NAMES[self.shard_index]]
    for x in xrange(count):
      keyspace_id = kid_list[x%len(kid_list)]
      cursor = vtgate_conn.cursor(
          tablet_type='master', keyspace=KEYSPACE_NAME,
          keyspace_ids=[pack_kid(keyspace_id)],
          writable=True)
      cursor.begin()
      cursor.execute(
          'insert into vt_insert_test (msg, keyspace_id) '
          'values (:msg, :keyspace_id)',
          {'msg': 'test %s' % x, 'keyspace_id': keyspace_id})
      cursor.commit()
    vtgate_conn.begin()
    vtgate_conn._execute(
        'delete from vt_insert_test', {},
        tablet_type='master', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    vtgate_conn.rollback()
    cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    rowcount = cursor.execute('select * from vt_insert_test', {})
    logging.debug('ROLLBACK TEST rowcount %d count %d', rowcount, count)
    self.assertEqual(
        rowcount, count,
        "Fetched rows(%d) != inserted rows(%d), rollback didn't work" %
        (rowcount, count))
    write_rows_to_shard(10, self.shard_index)
    vtgate_conn.close()

  def test_execute_entity_ids(self):
    vtgate_conn = get_connection()
    count = 10
    _delete_all(self.shard_index, 'vt_a')
    eid_map = {}
    kid_list = SHARD_KID_MAP[SHARD_NAMES[self.shard_index]]
    for x in xrange(count):
      keyspace_id = kid_list[x%len(kid_list)]
      eid_map[x] = pack_kid(keyspace_id)
      cursor = vtgate_conn.cursor(
          tablet_type='master', keyspace=KEYSPACE_NAME,
          keyspace_ids=[pack_kid(keyspace_id)],
          writable=True)
      cursor.begin()
      cursor.execute(
          'insert into vt_a (eid, id, keyspace_id) '
          'values (:eid, :id, :keyspace_id)',
          {'eid': x, 'id': x, 'keyspace_id': keyspace_id})
      cursor.commit()
    cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace=KEYSPACE_NAME, keyspace_ids=None)
    rowcount = cursor.execute(
        'select * from vt_a', {},
        entity_keyspace_id_map=eid_map, entity_column_name='id')
    self.assertEqual(rowcount, count, 'entity_ids works')
    vtgate_conn.close()

  def test_batch_read(self):
    vtgate_conn = get_connection()
    count = 10
    _delete_all(self.shard_index, 'vt_insert_test')
    shard_name = SHARD_NAMES[self.shard_index]
    kid_list = SHARD_KID_MAP[shard_name]
    for x in xrange(count):
      keyspace_id = kid_list[x%len(kid_list)]
      cursor = vtgate_conn.cursor(
          tablet_type='master', keyspace=KEYSPACE_NAME,
          keyspace_ids=[pack_kid(keyspace_id)],
          writable=True)
      cursor.begin()
      cursor.execute(
          'insert into vt_insert_test (msg, keyspace_id) '
          'values (:msg, :keyspace_id)',
          {'msg': 'test %s' % x, 'keyspace_id': keyspace_id})
      cursor.commit()
    _delete_all(self.shard_index, 'vt_a')
    for x in xrange(count):
      keyspace_id = kid_list[x%len(kid_list)]
      cursor = vtgate_conn.cursor(
          tablet_type='master', keyspace=KEYSPACE_NAME,
          keyspace_ids=[pack_kid(keyspace_id)],
          writable=True)
      cursor.begin()
      cursor.execute(
          'insert into vt_a (eid, id, keyspace_id) '
          'values (:eid, :id, :keyspace_id)',
          {'eid': x, 'id': x, 'keyspace_id': keyspace_id})
      cursor.commit()
    kid_list = [pack_kid(kid) for kid in kid_list]
    cursor = vtgate_conn.cursor(tablet_type='master', keyspace=None)

    # Test ExecuteBatchKeyspaceIds
    params_list = [
        dict(sql='select msg, keyspace_id from vt_insert_test',
             bind_variables={},
             keyspace=KEYSPACE_NAME, keyspace_ids=kid_list,
             shards=None),
        dict(sql='select eid + 100, id, keyspace_id from vt_a',
             bind_variables={},
             keyspace=KEYSPACE_NAME, keyspace_ids=kid_list,
             shards=None),
    ]
    cursor.executemany(sql=None, params_list=params_list)
    self.assertEqual(cursor.rowcount, count)
    msg_0, msg_1 = (row[0] for row in sorted(cursor.fetchall())[:2])
    self.assertEqual(msg_0, 'test 0')
    self.assertEqual(msg_1, 'test 1')
    self.assertTrue(cursor.nextset())
    eid_0_plus_100, eid_1_plus_100 = (
        row[0] for row in sorted(cursor.fetchall())[:2])
    self.assertEqual(eid_0_plus_100, 100)
    self.assertEqual(eid_1_plus_100, 101)
    self.assertFalse(cursor.nextset())

    # Test ExecuteBatchShards
    params_list = [
        dict(sql='select eid, id, keyspace_id from vt_a',
             bind_variables={},
             keyspace=KEYSPACE_NAME,
             keyspace_ids=None,
             shards=[shard_name]),
        dict(sql='select eid + 100, id, keyspace_id from vt_a',
             bind_variables={},
             keyspace=KEYSPACE_NAME,
             keyspace_ids=None,
             shards=[shard_name]),
    ]
    cursor.executemany(sql=None, params_list=params_list)
    self.assertEqual(cursor.rowcount, count)
    eid_0, eid_1 = (row[0] for row in sorted(cursor.fetchall())[:2])
    self.assertEqual(eid_0, 0)
    self.assertEqual(eid_1, 1)
    self.assertTrue(cursor.nextset())
    eid_0_plus_100, eid_1_plus_100 = (
        row[0] for row in sorted(cursor.fetchall())[:2])
    self.assertEqual(eid_0_plus_100, 100)
    self.assertEqual(eid_1_plus_100, 101)
    self.assertFalse(cursor.nextset())
    vtgate_conn.close()

  def test_batch_write(self):
    vtgate_conn = get_connection()
    cursor = vtgate_conn.cursor(tablet_type='master', keyspace=None)
    kid_list = SHARD_KID_MAP[SHARD_NAMES[self.shard_index]]
    all_ids = [pack_kid(kid) for kid in kid_list]
    count = 10
    cursor.executemany(
        sql=None,
        params_list=[
            dict(sql='delete from vt_insert_test', bind_variables=None,
                 keyspace=KEYSPACE_NAME, keyspace_ids=all_ids,
                 shards=None)])

    params_list = []
    for x in xrange(count):
      keyspace_id = kid_list[x%len(kid_list)]
      params_list.append(
          dict(sql=None,
               bind_variables=
               {'msg': 'test %s' % x, 'keyspace_id': keyspace_id},
               keyspace=KEYSPACE_NAME,
               keyspace_ids=[pack_kid(keyspace_id)],
               shards=None))
    cursor.executemany(
        sql='insert into vt_insert_test (msg, keyspace_id) '
        'values (:msg, :keyspace_id)',
        params_list=params_list)
    cursor.executemany(
        sql=None,
        params_list=[
            dict(sql='delete from vt_a', bind_variables=None,
                 keyspace=KEYSPACE_NAME, keyspace_ids=all_ids, shards=None)])
    params_list = []
    for x in xrange(count):
      keyspace_id = kid_list[x%len(kid_list)]
      sql = (
          'insert into vt_a (eid, id, keyspace_id) '
          'values (:eid, :id, :keyspace_id)')
      bind_variables = {'eid': x, 'id': x, 'keyspace_id': keyspace_id}
      keyspace = KEYSPACE_NAME
      keyspace_ids = [pack_kid(keyspace_id)]
      params_list.append(dict(
          sql=sql, bind_variables=bind_variables, keyspace=keyspace,
          keyspace_ids=keyspace_ids, shards=None))
    cursor.executemany(sql=None, params_list=params_list)
    _, rowcount, _, _ = vtgate_conn._execute(
        'select * from vt_insert_test', {},
        tablet_type='master', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    self.assertEqual(rowcount, count)
    _, rowcount, _, _ = vtgate_conn._execute(
        'select * from vt_a', {},
        tablet_type='master', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    self.assertEqual(rowcount, count)
    vtgate_conn.close()

  def test_streaming_fetchsubset(self):
    count = 30
    write_rows_to_shard(count, self.shard_index)
    # Fetch a subset of the total size.
    vtgate_conn = get_connection()

    def get_stream_cursor():
      return vtgate_conn.cursor(
          tablet_type='master', keyspace=KEYSPACE_NAME,
          keyranges=[self.keyrange],
          cursorclass=vtgate_cursor.StreamVTGateCursor)

    def fetch_first_10_rows(stream_cursor):
      stream_cursor.execute('select msg from vt_insert_test', {})
      rows = stream_cursor.fetchmany(size=10)
      self.assertEqual(rows, [('test %d' % x,) for x in xrange(10)])

    def fetch_next_10_rows(stream_cursor):
      rows = stream_cursor.fetchmany(size=10)
      self.assertEqual(rows, [('test %d' % x,) for x in xrange(10, 20)])

    # Open two streaming queries at the same time, fetch some from each,
    # and make sure they don't interfere with each other.
    stream_cursor_1 = get_stream_cursor()
    stream_cursor_2 = get_stream_cursor()
    fetch_first_10_rows(stream_cursor_1)
    fetch_first_10_rows(stream_cursor_2)
    fetch_next_10_rows(stream_cursor_1)
    fetch_next_10_rows(stream_cursor_2)
    stream_cursor_1.close()
    stream_cursor_2.close()
    vtgate_conn.close()

  def test_streaming_fetchall(self):
    count = 30
    write_rows_to_shard(count, self.shard_index)
    # Fetch all.
    vtgate_conn = get_connection()
    stream_cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace=KEYSPACE_NAME,
        keyranges=[self.keyrange],
        cursorclass=vtgate_cursor.StreamVTGateCursor)
    stream_cursor.execute('select * from vt_insert_test', {})
    rows = stream_cursor.fetchall()
    rowcount = len(list(rows))
    self.assertEqual(rowcount, count)
    stream_cursor.close()
    vtgate_conn.close()

  def test_streaming_fetchone(self):
    count = 30
    write_rows_to_shard(count, self.shard_index)
    # Fetch one.
    vtgate_conn = get_connection()
    stream_cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace=KEYSPACE_NAME,
        keyranges=[self.keyrange],
        cursorclass=vtgate_cursor.StreamVTGateCursor)
    stream_cursor.execute('select * from vt_insert_test', {})
    rows = stream_cursor.fetchone()
    self.assertTrue(isinstance(rows, tuple), 'Received a valid row')
    stream_cursor.close()
    vtgate_conn.close()

  def test_streaming_multishards(self):
    count = 30
    write_rows_to_shard(count, 0)
    write_rows_to_shard(count, 1)
    vtgate_conn = get_connection()
    stream_cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace=KEYSPACE_NAME,
        keyranges=[keyrange.KeyRange(
            keyrange_constants.NON_PARTIAL_KEYRANGE)],
        cursorclass=vtgate_cursor.StreamVTGateCursor)
    stream_cursor.execute('select * from vt_insert_test', {})
    rows = stream_cursor.fetchall()
    rowcount = len(list(rows))
    self.assertEqual(rowcount, count * 2)
    stream_cursor.close()
    vtgate_conn.close()

  def test_streaming_zero_results(self):
    vtgate_conn = get_connection()
    vtgate_conn.begin()
    vtgate_conn._execute(
        'delete from vt_insert_test', {},
        tablet_type='master', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    vtgate_conn.commit()
    # After deletion, should result zero.
    stream_cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace=KEYSPACE_NAME,
        keyranges=[self.keyrange],
        cursorclass=vtgate_cursor.StreamVTGateCursor)
    stream_cursor.execute('select * from vt_insert_test', {})
    rows = stream_cursor.fetchall()
    rowcount = len(list(rows))
    self.assertEqual(rowcount, 0)
    vtgate_conn.close()

  def test_interleaving(self):
    tablet_type = 'master'
    vtgate_conn = get_connection()
    vtgate_conn.begin()
    vtgate_conn._execute(
        'delete from vt_insert_test', {},
        tablet_type=tablet_type, keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    kid_list = SHARD_KID_MAP[SHARD_NAMES[self.shard_index]]
    count = len(kid_list)
    for x in xrange(count):
      keyspace_id = kid_list[x]
      vtgate_conn._execute(
          'insert into vt_insert_test (msg, keyspace_id) '
          'values (:msg, :keyspace_id)',
          {'msg': 'test %s' % x, 'keyspace_id': keyspace_id},
          tablet_type=tablet_type, keyspace_name=KEYSPACE_NAME,
          keyspace_ids=[pack_kid(keyspace_id)])
    vtgate_conn.commit()
    vtgate_conn2 = get_connection()
    query = (
        'select keyspace_id from vt_insert_test where keyspace_id = :kid')
    thd = threading.Thread(target=self._query_lots, args=(
        vtgate_conn2,
        query,
        {'kid': kid_list[0]},
        KEYSPACE_NAME,
        tablet_type,
        [pack_kid(kid_list[0])]))
    thd.start()
    for i in xrange(count):
      (result, _, _, _) = vtgate_conn._execute(
          query,
          {'kid': kid_list[i]},
          tablet_type=tablet_type, keyspace_name=KEYSPACE_NAME,
          keyspace_ids=[pack_kid(kid_list[i])])
      self.assertEqual(result, [(kid_list[i],)])
      if i % 10 == 0:
        generator, _ = vtgate_conn._stream_execute(
            query, {'kid': kid_list[i]},
            tablet_type=tablet_type, keyspace_name=KEYSPACE_NAME,
            keyspace_ids=[pack_kid(kid_list[i])])
        for result in generator:
          self.assertEqual(result, (kid_list[i],))
    thd.join()
    vtgate_conn.close()
    vtgate_conn2.close()

  def test_sequence(self):
    tablet_type = 'master'
    try:
      vtgate_conn = get_connection()
      # Special-cased initialization of sequence to shard 0.
      vtgate_conn.begin()
      vtgate_conn._execute(
          INIT_VT_SEQ, {'keyspace_id': 0},
          tablet_type=tablet_type, keyspace_name=KEYSPACE_NAME,
          keyspace_ids=[pack_kid(0)])
      vtgate_conn.commit()
      want = 1
      for _ in xrange(10):
        result, _, _, _ = vtgate_conn._execute(
            'select next :n values for vt_seq', {'n': 2},
            tablet_type=tablet_type, keyspace_name=KEYSPACE_NAME,
            keyspace_ids=[pack_kid(0)])
        self.assertEqual(result[0][0], want)
        want += 2
    except Exception, e:  # pylint: disable=broad-except
      self.fail('Failed with error %s %s' % (str(e), traceback.format_exc()))
    vtgate_conn.close()

  def test_field_types(self):
    vtgate_conn = get_connection()
    _delete_all(self.shard_index, 'vt_field_types')
    count = 10
    base_uint = int('8' + '0' * 15, base=16)
    kid_list = SHARD_KID_MAP[SHARD_NAMES[self.shard_index]]
    for x in xrange(1, count):
      keyspace_id = kid_list[count % len(kid_list)]
      cursor = vtgate_conn.cursor(
          tablet_type='master', keyspace=KEYSPACE_NAME,
          keyspace_ids=[pack_kid(keyspace_id)],
          writable=True)
      cursor.begin()
      cursor.execute(
          'insert into vt_field_types '
          '(uint_val, str_val, unicode_val, float_val, keyspace_id) '
          'values (:uint_val, :str_val, :unicode_val, '
          ':float_val, :keyspace_id)',
          {'uint_val': base_uint + x, 'str_val': 'str_%d' % x,
           'unicode_val': unicode('str_%d' % x), 'float_val': x * 1.2,
           'keyspace_id': keyspace_id})
      cursor.commit()
    cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    rowcount = cursor.execute('select * from vt_field_types', {})
    field_names = [f[0] for f in cursor.description]
    self.assertEqual(rowcount, count -1, "rowcount doesn't match")
    id_list = []
    uint_val_list = []
    str_val_list = []
    unicode_val_list = []
    float_val_list = []
    for r in cursor.results:
      row = DBRow(field_names, r)
      id_list.append(row.id)
      uint_val_list.append(row.uint_val)
      str_val_list.append(row.str_val)
      unicode_val_list.append(row.unicode_val)
      float_val_list.append(row.float_val)

    # iterable type checks - list, tuple, set are supported.
    query = 'select * from vt_field_types where id in ::id_1'
    rowcount = cursor.execute(query, {'id_1': id_list})
    self.assertEqual(rowcount, len(id_list), "rowcount doesn't match")
    rowcount = cursor.execute(query, {'id_1': tuple(id_list)})
    self.assertEqual(rowcount, len(id_list), "rowcount doesn't match")
    rowcount = cursor.execute(query, {'id_1': set(id_list)})
    self.assertEqual(rowcount, len(id_list), "rowcount doesn't match")
    for r in cursor.results:
      row = DBRow(field_names, r)
      self.assertIsInstance(row.id, (int, long))

    # received field types same as input.
    # uint
    query = 'select * from vt_field_types where uint_val in ::uint_val_1'
    rowcount = cursor.execute(query, {'uint_val_1': uint_val_list})
    self.assertEqual(rowcount, len(uint_val_list), "rowcount doesn't match")
    for _, r in enumerate(cursor.results):
      row = DBRow(field_names, r)
      self.assertIsInstance(row.uint_val, long)
      self.assertGreaterEqual(
          row.uint_val, base_uint, 'uint value not in correct range')

    # str
    query = 'select * from vt_field_types where str_val in ::str_val_1'
    rowcount = cursor.execute(query, {'str_val_1': str_val_list})
    self.assertEqual(rowcount, len(str_val_list), "rowcount doesn't match")
    for r in cursor.results:
      row = DBRow(field_names, r)
      self.assertIsInstance(row.str_val, str)

    # unicode str
    query = (
        'select * from vt_field_types where unicode_val in ::unicode_val_1')
    rowcount = cursor.execute(query, {'unicode_val_1': unicode_val_list})
    self.assertEqual(
        rowcount, len(unicode_val_list), "rowcount doesn't match")
    for r in cursor.results:
      row = DBRow(field_names, r)
      self.assertIsInstance(row.unicode_val, basestring)

    # deliberately eliminating the float test since it is flaky due
    # to mysql float precision handling.

    vtgate_conn.close()

  def _query_lots(
      self, conn, query, bind_vars, keyspace_name, tablet_type, keyspace_ids):
    for _ in xrange(500):
      result, _, _, _ = conn._execute(
          query, bind_vars,
          tablet_type=tablet_type, keyspace_name=keyspace_name,
          keyspace_ids=keyspace_ids)
      self.assertEqual(result, [tuple(bind_vars.values())])

  def test_vschema_vars(self):
    v = utils.vtgate.get_vars()
    self.assertIn('VtgateVSchemaCounts', v)
    self.assertIn('Reload', v['VtgateVSchemaCounts'])
    self.assertTrue(v['VtgateVSchemaCounts']['Reload'] > 0)
    self.assertIn('WatchError', v['VtgateVSchemaCounts'])
    self.assertTrue(v['VtgateVSchemaCounts']['WatchError'] > 0)
    self.assertNotIn('Parsing', v['VtgateVSchemaCounts'])


class TestFailures(BaseTestCase):

  def setUp(self):
    super(TestFailures, self).setUp()
    self.shard_index = 1
    self.keyrange = keyrange.KeyRange(SHARD_NAMES[self.shard_index])
    self.master_tablet = shard_1_master
    self.replica_tablet = shard_1_replica1
    self.replica_tablet2 = shard_1_replica2

  def tablet_start(self, tablet_obj, tablet_type, lameduck_period='0.5s',
                   grace_period=None):
    if grace_period is None:
      # If grace_period is not specified, use whatever default is defined in
      # start_vttablet() itself.
      tablet_obj.start_vttablet(lameduck_period=lameduck_period,
                                init_tablet_type=tablet_type,
                                init_keyspace=KEYSPACE_NAME,
                                init_shard=SHARD_NAMES[self.shard_index])
    else:
      tablet_obj.start_vttablet(lameduck_period=lameduck_period,
                                grace_period=grace_period,
                                init_tablet_type=tablet_type,
                                init_keyspace=KEYSPACE_NAME,
                                init_shard=SHARD_NAMES[self.shard_index])

  def test_status_with_error(self):
    """Tests that the status page loads correctly after a VTGate error."""
    vtgate_conn = get_connection()
    cursor = vtgate_conn.cursor(
        tablet_type='replica', keyspace='INVALID_KEYSPACE', keyspace_ids=['0'])
    # We expect to see a DatabaseError due to an invalid keyspace
    with self.assertRaises(dbexceptions.DatabaseError):
      cursor.execute('select * from vt_insert_test', {})
    vtgate_conn.close()

    # Page should have loaded successfully
    self.assertIn('</html>', utils.vtgate.get_status())

  def test_tablet_restart_read(self):
    # Since we're going to kill the tablet, there will be a race between the
    # client timeout here and the vtgate->vttablet connection timeout, so we
    # increase it for this test.
    vtgate_conn = get_connection(timeout=30)
    self.replica_tablet.kill_vttablet()
    self.replica_tablet2.kill_vttablet()
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn._execute(
          'select 1 from vt_insert_test', {},
          tablet_type='replica', keyspace_name=KEYSPACE_NAME,
          keyranges=[self.keyrange])
    self.tablet_start(self.replica_tablet, 'replica')
    self.tablet_start(self.replica_tablet2, 'replica')
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        2)
    try:
      _ = vtgate_conn._execute(
          'select 1 from vt_insert_test', {},
          tablet_type='replica', keyspace_name=KEYSPACE_NAME,
          keyranges=[self.keyrange])
    except Exception, e:  # pylint: disable=broad-except
      self.fail('Communication with shard %s replica failed with error %s' %
                (SHARD_NAMES[self.shard_index], str(e)))
    vtgate_conn.close()

  def test_vtgate_restart_read(self):
    vtgate_conn = get_connection()
    port = utils.vtgate.port
    utils.vtgate.kill()
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn._execute(
          'select 1 from vt_insert_test', {},
          tablet_type='replica', keyspace_name=KEYSPACE_NAME,
          keyranges=[self.keyrange])
    restart_vtgate(port)
    vtgate_conn = get_connection()
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        2)
    vtgate_conn._execute(
        'select 1 from vt_insert_test', {},
        tablet_type='replica', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    vtgate_conn.close()

  def test_tablet_restart_stream_execute(self):
    # Since we're going to kill the tablet, there will be a race between the
    # client timeout here and the vtgate->vttablet connection timeout, so we
    # increase it for this test.
    vtgate_conn = get_connection(timeout=30)
    stream_cursor = vtgate_conn.cursor(
        tablet_type='replica', keyspace=KEYSPACE_NAME,
        keyranges=[self.keyrange],
        cursorclass=vtgate_cursor.StreamVTGateCursor)
    self.replica_tablet.kill_vttablet()
    self.replica_tablet2.kill_vttablet()
    with self.assertRaises(dbexceptions.DatabaseError):
      stream_cursor.execute('select * from vt_insert_test', {})
    self.tablet_start(self.replica_tablet, 'replica')
    self.tablet_start(self.replica_tablet2, 'replica')
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        2)
    try:
      stream_cursor.execute('select * from vt_insert_test', {})
    except Exception, e:  # pylint: disable=broad-except
      self.fail('Communication with shard0 replica failed with error %s' %
                str(e))
    vtgate_conn.close()

  def test_vtgate_restart_stream_execute(self):
    vtgate_conn = get_connection()
    stream_cursor = vtgate_conn.cursor(
        tablet_type='replica', keyspace=KEYSPACE_NAME,
        keyranges=[self.keyrange],
        cursorclass=vtgate_cursor.StreamVTGateCursor)
    port = utils.vtgate.port
    utils.vtgate.kill()
    with self.assertRaises(dbexceptions.DatabaseError):
      stream_cursor.execute('select * from vt_insert_test', {})
    vtgate_conn.close()

    restart_vtgate(port)
    vtgate_conn = get_connection()
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        2)
    stream_cursor = vtgate_conn.cursor(
        tablet_type='replica', keyspace=KEYSPACE_NAME,
        keyranges=[self.keyrange],
        cursorclass=vtgate_cursor.StreamVTGateCursor)
    try:
      stream_cursor.execute('select * from vt_insert_test', {})
    except Exception, e:  # pylint: disable=broad-except
      self.fail('Communication with shard0 replica failed with error %s' %
                str(e))
    vtgate_conn.close()

  # vtgate begin doesn't make any back-end connections to
  # vttablet so the kill and restart shouldn't have any effect.
  def test_tablet_restart_begin(self):
    vtgate_conn = get_connection()
    self.master_tablet.kill_vttablet()
    vtgate_conn.begin()
    self.tablet_start(self.master_tablet, 'replica')
    wait_for_endpoints(
        '%s.%s.master' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        1)
    vtgate_conn.begin()
    # this succeeds only if retry_count > 0
    vtgate_conn._execute(
        'delete from vt_insert_test', {},
        tablet_type='master', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    vtgate_conn.commit()
    vtgate_conn.close()

  def test_vtgate_restart_begin(self):
    vtgate_conn = get_connection()
    port = utils.vtgate.port
    utils.vtgate.kill()
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn.begin()
    restart_vtgate(port)
    vtgate_conn = get_connection()
    wait_for_endpoints(
        '%s.%s.master' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        1)
    vtgate_conn.begin()
    vtgate_conn.close()

  def test_tablet_fail_write(self):
    # Since we're going to kill the tablet, there will be a race between the
    # client timeout here and the vtgate->vttablet connection timeout, so we
    # increase it for this test.
    vtgate_conn = get_connection(timeout=30)
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn.begin()
      self.master_tablet.kill_vttablet()
      vtgate_conn._execute(
          'delete from vt_insert_test', {},
          tablet_type='master', keyspace_name=KEYSPACE_NAME,
          keyranges=[self.keyrange])
      vtgate_conn.commit()
    self.tablet_start(self.master_tablet, 'replica')
    wait_for_endpoints(
        '%s.%s.master' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        1)
    vtgate_conn.begin()
    vtgate_conn._execute(
        'delete from vt_insert_test', {},
        tablet_type='master', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    vtgate_conn.commit()
    vtgate_conn.close()

  def _get_non_vtgate_errors(self):
    v = utils.vtgate.get_vars()
    if 'VtgateInfoErrorCounts' not in v:
      return 0
    if 'NonVtgateErrors' not in v['VtgateInfoErrorCounts']:
      return 0
    return v['VtgateInfoErrorCounts']['NonVtgateErrors']

  def test_error_on_dml(self):
    vtgate_conn = get_connection()
    vtgate_conn.begin()
    keyspace_id = SHARD_KID_MAP[SHARD_NAMES[
        (self.shard_index+1)%len(SHARD_NAMES)
        ]][0]
    try:
      vtgate_conn._execute(
          'insert into vt_insert_test values(:msg, :keyspace_id)',
          {'msg': 'test4', 'keyspace_id': keyspace_id},
          tablet_type='master', keyspace_name=KEYSPACE_NAME,
          keyranges=[self.keyrange])
      vtgate_conn.commit()
      self.fail('Failed to raise DatabaseError exception')
    except dbexceptions.DatabaseError:
      # FIXME(alainjobart) add a method to get the session to vtgate_client,
      # instead of poking into it like this.
      logging.info('Shard session: %s', vtgate_conn.session)
      transaction_id = vtgate_conn.session.shard_sessions[0].transaction_id
      self.assertTrue(transaction_id != 0)
    except Exception, e:  # pylint: disable=broad-except
      self.fail('Expected DatabaseError as exception, got %s' % str(e))
    finally:
      vtgate_conn.rollback()
    vtgate_conn.close()

  def test_vtgate_fail_write(self):
    # use a shorter timeout, we know we're going to hit it (twice in fact,
    # once for the _execute, once for the close's rollback).
    vtgate_conn = get_connection(timeout=5.0)
    port = utils.vtgate.port
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn.begin()
      utils.vtgate.kill()
      vtgate_conn._execute(
          'delete from vt_insert_test', {},
          tablet_type='master', keyspace_name=KEYSPACE_NAME,
          keyranges=[self.keyrange])
      vtgate_conn.commit()

    # Note this tests a connection with an interrupted transaction can
    # still be closed.
    vtgate_conn.close()

    restart_vtgate(port)
    vtgate_conn = get_connection()
    wait_for_endpoints(
        '%s.%s.master' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        1)
    vtgate_conn.begin()
    vtgate_conn._execute(
        'delete from vt_insert_test', {},
        tablet_type='master', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    vtgate_conn.commit()
    vtgate_conn.close()

  # test timeout between py client and vtgate
  def test_vtgate_timeout(self):
    vtgate_conn = get_connection(timeout=3.0)
    with self.assertRaises(dbexceptions.TimeoutError):
      vtgate_conn._execute(
          'select sleep(4) from dual', {},
          tablet_type='replica', keyspace_name=KEYSPACE_NAME,
          keyranges=[self.keyrange])
    vtgate_conn.close()

    vtgate_conn = get_connection(timeout=3.0)
    with self.assertRaises(dbexceptions.TimeoutError):
      vtgate_conn._execute(
          'select sleep(4) from dual', {},
          tablet_type='master', keyspace_name=KEYSPACE_NAME,
          keyranges=[self.keyrange])
    vtgate_conn.close()

     # Currently this is causing vttablet to become unreachable at
     # the timeout boundary and kill any query being executed
     # at the time. Prevent flakiness in other tests by sleeping
     # until the query times out.
     # TODO(b/17733518)
    time.sleep(3)

  # test timeout between vtgate and vttablet
  # the timeout is set to 5 seconds
  def test_tablet_timeout(self):
    # this test only makes sense if there is a shorter/protective timeout
    # set for vtgate-vttablet connection.
    # TODO(liguo): evaluate if we want such a timeout
    return
    vtgate_conn = get_connection()  # pylint: disable=unreachable
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn.begin()
      vtgate_conn._execute(
          'select sleep(7) from dual', {},
          tablet_type='replica', keyspace_name=KEYSPACE_NAME,
          keyranges=[self.keyrange])
    vtgate_conn.close()

    vtgate_conn = get_connection()
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn.begin()
      vtgate_conn._execute(
          'select sleep(7) from dual', {},
          tablet_type='master', keyspace_name=KEYSPACE_NAME,
          keyranges=[self.keyrange])
    vtgate_conn.close()

  # Test the case that no query sent during tablet shuts down (single tablet)
  def test_restart_mysql_tablet_idle(self):
    self.replica_tablet2.kill_vttablet()
    vtgate_conn = get_connection()
    utils.wait_procs([self.replica_tablet.shutdown_mysql(),])
    try:
      vtgate_conn._execute(
          'select 1 from vt_insert_test', {},
          tablet_type='replica', keyspace_name=KEYSPACE_NAME,
          keyranges=[self.keyrange])
      self.fail('DatabaseError should have been raised')
    except Exception, e:  # pylint: disable=broad-except
      self.assertIsInstance(e, dbexceptions.DatabaseError)
      self.assertNotIsInstance(e, dbexceptions.IntegrityError)
      self.assertNotIsInstance(e, dbexceptions.OperationalError)
      self.assertNotIsInstance(e, dbexceptions.TimeoutError)

    utils.wait_procs([self.replica_tablet.start_mysql(),])
    # then restart replication, and write data, make sure we go back to healthy
    for t in [self.replica_tablet]:
      utils.run_vtctl(['StartSlave', t.tablet_alias])
      utils.run_vtctl(['RunHealthCheck', t.tablet_alias],
                      auto_log=True)
      t.wait_for_vttablet_state('SERVING')
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        1)
    vtgate_conn._execute(
        'select 1 from vt_insert_test', {},
        tablet_type='replica', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    vtgate_conn.close()

    self.tablet_start(self.replica_tablet2, 'replica')
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        2)

  # Test the case that there are queries sent during vttablet shuts down,
  # and all querys fail because there is only one vttablet.
  def test_restart_mysql_tablet_queries(self):
    vtgate_conn = get_connection()
    utils.wait_procs([self.replica_tablet.shutdown_mysql(),])
    utils.wait_procs([self.replica_tablet2.shutdown_mysql(),])
    try:
      vtgate_conn._execute(
          'select 1 from vt_insert_test', {},
          tablet_type='replica', keyspace_name=KEYSPACE_NAME,
          keyranges=[self.keyrange])
      self.fail('DatabaseError should have been raised')
    except Exception, e:  # pylint: disable=broad-except
      self.assertIsInstance(e, dbexceptions.DatabaseError)
      self.assertNotIsInstance(e, dbexceptions.IntegrityError)
      self.assertNotIsInstance(e, dbexceptions.OperationalError)
      self.assertNotIsInstance(e, dbexceptions.TimeoutError)
    utils.wait_procs([self.replica_tablet.start_mysql(),])
    utils.wait_procs([self.replica_tablet2.start_mysql(),])
    # then restart replication, and write data, make sure we go back to healthy
    for t in [self.replica_tablet, self.replica_tablet2]:
      utils.run_vtctl(['StartSlave', t.tablet_alias])
      utils.run_vtctl(['RunHealthCheck', t.tablet_alias],
                      auto_log=True)
      t.wait_for_vttablet_state('SERVING')
    self.replica_tablet2.kill_vttablet()
    replica_tablet_proc = self.replica_tablet.kill_vttablet(wait=False)
    # send query while vttablet is in lameduck, should fail as no vttablet
    time.sleep(0.1)  # wait a short while so vtgate gets the health check
    try:
      vtgate_conn._execute(
          'select 1 from vt_insert_test', {},
          tablet_type='replica', keyspace_name=KEYSPACE_NAME,
          keyranges=[self.keyrange])
      self.fail('DatabaseError should have been raised')
    except Exception, e:  # pylint: disable=broad-except
      self.assertIsInstance(e, dbexceptions.DatabaseError)
      self.assertNotIsInstance(e, dbexceptions.IntegrityError)
      self.assertNotIsInstance(e, dbexceptions.OperationalError)
      self.assertNotIsInstance(e, dbexceptions.TimeoutError)
    # Wait for original tablet to finish before restarting.
    replica_tablet_proc.wait()
    self.tablet_start(self.replica_tablet, 'replica')
    self.tablet_start(self.replica_tablet2, 'replica')
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        2)
    # as the cached vtgate-tablet conn was marked down, it should succeed
    vtgate_conn._execute(
        'select 1 from vt_insert_test', {},
        tablet_type='replica', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    vtgate_conn.close()

  # Test the case that there are queries sent during one vttablet shuts down,
  # and all queries succeed because there is another vttablet.
  def test_restart_mysql_tablet_queries_multi_tablets(self):
    vtgate_conn = get_connection()
    utils.wait_procs([self.replica_tablet.shutdown_mysql(),])
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        1)
    # should retry on tablet2 and succeed
    vtgate_conn._execute(
        'select 1 from vt_insert_test', {},
        tablet_type='replica', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    utils.wait_procs([self.replica_tablet.start_mysql(),])
    # then restart replication, and write data, make sure we go back to healthy
    for t in [self.replica_tablet]:
      utils.run_vtctl(['StartSlave', t.tablet_alias])
      utils.run_vtctl(['RunHealthCheck', t.tablet_alias],
                      auto_log=True)
      t.wait_for_vttablet_state('SERVING')
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        2)
    vtgate_conn._execute(
        'select 1 from vt_insert_test', {},
        tablet_type='replica', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    # kill tablet2 and leave it in lameduck mode
    replica_tablet2_proc = self.replica_tablet2.kill_vttablet(wait=False)
    time.sleep(0.1)
    # send query while tablet2 is in lameduck, should retry on tablet1
    tablet1_vars = utils.get_vars(self.replica_tablet.port)
    t1_query_count_before = int(tablet1_vars['Queries']['TotalCount'])
    vtgate_conn._execute(
        'select 1 from vt_insert_test', {},
        tablet_type='replica', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    tablet1_vars = utils.get_vars(self.replica_tablet.port)
    t1_query_count_after = int(tablet1_vars['Queries']['TotalCount'])
    self.assertEquals(t1_query_count_after-t1_query_count_before, 1)
    # Wait for tablet2 to go down.
    replica_tablet2_proc.wait()
    # send another query, should also succeed on tablet1
    tablet1_vars = utils.get_vars(self.replica_tablet.port)
    t1_query_count_before = int(tablet1_vars['Queries']['TotalCount'])
    vtgate_conn._execute(
        'select 1 from vt_insert_test', {},
        tablet_type='replica', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    tablet1_vars = utils.get_vars(self.replica_tablet.port)
    t1_query_count_after = int(tablet1_vars['Queries']['TotalCount'])
    self.assertEquals(t1_query_count_after-t1_query_count_before, 1)
    # start tablet2
    self.tablet_start(self.replica_tablet2, 'replica')
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        2)
    # query should succeed on either tablet
    tablet1_vars = utils.get_vars(self.replica_tablet.port)
    t1_query_count_before = int(tablet1_vars['Queries']['TotalCount'])
    tablet2_vars = utils.get_vars(self.replica_tablet2.port)
    t2_query_count_before = int(tablet2_vars['Queries']['TotalCount'])
    vtgate_conn._execute(
        'select 1 from vt_insert_test', {},
        tablet_type='replica', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    tablet1_vars = utils.get_vars(self.replica_tablet.port)
    t1_query_count_after = int(tablet1_vars['Queries']['TotalCount'])
    tablet2_vars = utils.get_vars(self.replica_tablet2.port)
    t2_query_count_after = int(tablet2_vars['Queries']['TotalCount'])
    self.assertEquals(t1_query_count_after-t1_query_count_before
                      +t2_query_count_after-t2_query_count_before, 1)
    vtgate_conn.close()

  # Test the case that there are queries sent during one vttablet is killed,
  # and all queries succeed because there is another vttablet.
  def test_kill_mysql_tablet_queries_multi_tablets(self):
    vtgate_conn = get_connection()
    utils.wait_procs([self.replica_tablet.shutdown_mysql(),])
    # should execute on tablet2 and succeed
    tablet2_vars = utils.get_vars(self.replica_tablet2.port)
    t2_query_count_before = int(tablet2_vars['Queries']['TotalCount'])
    vtgate_conn._execute(
        'select 1 from vt_insert_test', {},
        tablet_type='replica', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    tablet2_vars = utils.get_vars(self.replica_tablet2.port)
    t2_query_count_after = int(tablet2_vars['Queries']['TotalCount'])
    self.assertEquals(t2_query_count_after-t2_query_count_before, 1)
    # start tablet1 mysql
    utils.wait_procs([self.replica_tablet.start_mysql(),])
    # then restart replication, and write data, make sure we go back to healthy
    for t in [self.replica_tablet]:
      utils.run_vtctl(['StartSlave', t.tablet_alias])
      utils.run_vtctl(['RunHealthCheck', t.tablet_alias],
                      auto_log=True)
      t.wait_for_vttablet_state('SERVING')
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        2)
    # query should succeed
    vtgate_conn._execute(
        'select 1 from vt_insert_test', {},
        tablet_type='replica', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    # hard kill tablet2
    self.replica_tablet2.hard_kill_vttablet()
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        1)
    # send another query, should succeed on tablet1
    tablet1_vars = utils.get_vars(self.replica_tablet.port)
    t1_query_count_before = int(tablet1_vars['Queries']['TotalCount'])
    vtgate_conn._execute(
        'select 1 from vt_insert_test', {},
        tablet_type='replica', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    tablet1_vars = utils.get_vars(self.replica_tablet.port)
    t1_query_count_after = int(tablet1_vars['Queries']['TotalCount'])
    self.assertEquals(t1_query_count_after-t1_query_count_before, 1)
    # start tablet2
    self.tablet_start(self.replica_tablet2, 'replica')
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        2)
    # query should succeed on either tablet
    tablet1_vars = utils.get_vars(self.replica_tablet.port)
    t1_query_count_before = int(tablet1_vars['Queries']['TotalCount'])
    tablet2_vars = utils.get_vars(self.replica_tablet2.port)
    t2_query_count_before = int(tablet2_vars['Queries']['TotalCount'])
    vtgate_conn._execute(
        'select 1 from vt_insert_test', {},
        tablet_type='replica', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    tablet1_vars = utils.get_vars(self.replica_tablet.port)
    t1_query_count_after = int(tablet1_vars['Queries']['TotalCount'])
    tablet2_vars = utils.get_vars(self.replica_tablet2.port)
    t2_query_count_after = int(tablet2_vars['Queries']['TotalCount'])
    self.assertEquals(t1_query_count_after-t1_query_count_before
                      +t2_query_count_after-t2_query_count_before, 1)
    vtgate_conn.close()

  def test_bind_vars_in_exception_message(self):
    vtgate_conn = get_connection()
    keyspace_id = None

    count = 1
    vtgate_conn.begin()
    vtgate_conn._execute(
        'delete from vt_a', {},
        tablet_type='master', keyspace_name=KEYSPACE_NAME,
        keyranges=[keyrange.KeyRange(SHARD_NAMES[self.shard_index])])
    vtgate_conn.commit()
    eid_map = {}
    # start transaction
    vtgate_conn.begin()
    kid_list = SHARD_KID_MAP[SHARD_NAMES[self.shard_index]]

    # kill vttablet
    self.master_tablet.kill_vttablet()

    try:
      # perform write, this should fail
      for x in xrange(count):
        keyspace_id = kid_list[x%len(kid_list)]
        eid_map[x] = str(keyspace_id)
        vtgate_conn._execute(
            'insert into vt_a (eid, id, keyspace_id) '
            'values (:eid, :id, :keyspace_id)',
            {'eid': x, 'id': x, 'keyspace_id': keyspace_id},
            tablet_type='master', keyspace_name=KEYSPACE_NAME,
            keyspace_ids=[pack_kid(keyspace_id)])
      vtgate_conn.commit()
    except Exception, e:  # pylint: disable=broad-except
      # check that bind var value is not present in exception message.
      if str(keyspace_id) in str(e):
        self.fail('bind_vars present in the exception message')
    finally:
      vtgate_conn.rollback()
    # Start master tablet again
    self.tablet_start(self.master_tablet, 'replica')
    wait_for_endpoints(
        '%s.%s.master' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        1)
    vtgate_conn.close()

  def test_fail_fast_when_no_serving_tablets(self):
    """Verify VtGate requests fail-fast when tablets are unavailable.

    When there are no SERVING tablets available to serve a request,
    VtGate should fail-fast (returning an appropriate error) without
    waiting around till the request deadline expires.
    """
    tablet_type = 'replica'
    keyranges = [keyrange.KeyRange(SHARD_NAMES[self.shard_index])]
    query = 'select * from vt_insert_test'

    # Execute a query to warm VtGate's caches for connections and endpoints
    get_rtt(KEYSPACE_NAME, query, tablet_type, keyranges)

    # Shutdown mysql and ensure tablet is in NOT_SERVING state
    utils.wait_procs([self.replica_tablet.shutdown_mysql(),])
    utils.wait_procs([self.replica_tablet2.shutdown_mysql(),])

    try:
      get_rtt(KEYSPACE_NAME, query, tablet_type, keyranges)
      self.replica_tablet.wait_for_vttablet_state('NOT_SERVING')
      self.replica_tablet2.wait_for_vttablet_state('NOT_SERVING')
    except Exception:  # pylint: disable=broad-except
      self.fail('unable to set tablet to NOT_SERVING state')

    # Fire off a few requests in parallel
    num_requests = 10
    pool = ThreadPool(processes=num_requests)
    async_results = []
    for _ in range(num_requests):
      async_result = pool.apply_async(
          get_rtt, (KEYSPACE_NAME, query, tablet_type, keyranges))
      async_results.append(async_result)

    # Fetch all round trip times and verify max
    rt_times = []
    for async_result in async_results:
      rt_times.append(async_result.get())
    # The true upper limit is 2 seconds (1s * 2 retries as in
    # utils.py). To account for network latencies and other variances,
    # we keep an upper bound of 3 here.
    self.assertTrue(
        max(rt_times) < 3,
        'at least one request did not fail-fast; round trip times: %s' %
        rt_times)

    # Restart tablet and put it back to SERVING state
    utils.wait_procs([self.replica_tablet.start_mysql(),])
    utils.wait_procs([self.replica_tablet2.start_mysql(),])
    # then restart replication, and write data, make sure we go back to healthy
    for t in [self.replica_tablet, self.replica_tablet2]:
      utils.run_vtctl(['StartSlave', t.tablet_alias])
      utils.run_vtctl(['RunHealthCheck', t.tablet_alias],
                      auto_log=True)
      t.wait_for_vttablet_state('SERVING')
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        2)

  def test_lameduck_ongoing_query_single(self):
    self._test_lameduck_ongoing_query_single(0)

  def test_lameduck_ongoing_query_single_grace_period(self):
    self._test_lameduck_ongoing_query_single(2)

  def _test_lameduck_ongoing_query_single(self, grace_period):
    # disable the second replica, we'll only use the first one
    utils.wait_procs([self.replica_tablet2.shutdown_mysql(),])
    utils.run_vtctl(['RunHealthCheck', self.replica_tablet2.tablet_alias],
                    auto_log=True)
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        1)

    # re-configure the first tablet with a grace period
    self.replica_tablet.kill_vttablet()
    self.tablet_start(self.replica_tablet, 'replica',
                      lameduck_period='5s',
                      grace_period='%ds'%grace_period)
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        1)

    # make sure query can go through tablet1
    tablet1_vars = utils.get_vars(self.replica_tablet.port)
    t1_query_count_before = int(tablet1_vars['Queries']['TotalCount'])
    vtgate_conn = get_connection()
    try:
      vtgate_conn._execute(
          'select 1 from vt_insert_test', {},
          tablet_type='replica', keyspace_name=KEYSPACE_NAME,
          keyranges=[self.keyrange])
    except Exception, e:  # pylint: disable=broad-except
      self.fail('Failed with error %s %s' % (str(e), traceback.format_exc()))
    tablet1_vars = utils.get_vars(self.replica_tablet.port)
    t1_query_count_after = int(tablet1_vars['Queries']['TotalCount'])
    self.assertEquals(t1_query_count_after-t1_query_count_before, 1)
    # start a long running query
    num_requests = 10
    pool = ThreadPool(processes=num_requests)
    async_results = []
    for _ in range(5):
      async_result = pool.apply_async(
          send_long_query, (KEYSPACE_NAME, 'replica', [self.keyrange], 2))
      async_results.append(async_result)
    # soft kill vttablet
    # **should wait till previous queries are sent out**
    time.sleep(1)
    replica_tablet_proc = self.replica_tablet.kill_vttablet(wait=False)
    # Send query while vttablet is in lameduck.
    time.sleep(0.1)
    # With discoverygateway, it should fail regardless of grace period,
    # because vttablet broadcasts that it's unhealthy, and vtgate should
    # remove it immediately.
    try:
      vtgate_conn._execute(
          'select 1 from vt_insert_test', {},
          tablet_type='replica', keyspace_name=KEYSPACE_NAME,
          keyranges=[self.keyrange])
      self.fail('DatabaseError should have been raised')
    except Exception, e:  # pylint: disable=broad-except
      self.assertIsInstance(e, dbexceptions.DatabaseError)
      self.assertNotIsInstance(e, dbexceptions.IntegrityError)
      self.assertNotIsInstance(e, dbexceptions.OperationalError)
      self.assertNotIsInstance(e, dbexceptions.TimeoutError)
    # Fetch all ongoing query results
    query_results = []
    for async_result in async_results:
      query_results.append(async_result.get())
    # all should succeed
    for query_result in query_results:
      self.assertTrue(query_result)
    # Wait for the old replica_tablet to exit.
    replica_tablet_proc.wait()
    # start tablet1
    self.tablet_start(self.replica_tablet, 'replica')
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        1)
    # send another query, should succeed on tablet1
    vtgate_conn._execute(
        'select 1 from vt_insert_test', {},
        tablet_type='replica', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    vtgate_conn.close()

    # restart tablet2
    utils.wait_procs([self.replica_tablet2.start_mysql(),])
    utils.run_vtctl(['StartSlave', self.replica_tablet2.tablet_alias])
    utils.run_vtctl(['RunHealthCheck', self.replica_tablet2.tablet_alias],
                    auto_log=True)
    self.replica_tablet2.wait_for_vttablet_state('SERVING')
    wait_for_endpoints(
        '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[self.shard_index]),
        2)


# Return round trip time for a VtGate query, ignore any errors
def get_rtt(keyspace, query, tablet_type, keyranges):
  vtgate_conn = get_connection()
  cursor = vtgate_conn.cursor(
      tablet_type=tablet_type, keyspace=keyspace, keyranges=keyranges)
  start = time.time()
  try:
    cursor.execute(query, {})
  except Exception:  # pylint: disable=broad-except
    pass
  duration = time.time() - start
  vtgate_conn.close()
  return duration


# Send out a long query, return if it succeeds.
def send_long_query(keyspace, tablet_type, keyranges, delay):
  try:
    vtgate_conn = get_connection()
    cursor = vtgate_conn.cursor(
        tablet_type=tablet_type, keyspace=keyspace, keyranges=keyranges)
    query = 'select sleep(%s) from dual' % str(delay)
    try:
      cursor.execute(query, {})
    except Exception:  # pylint: disable=broad-except
      return False
    vtgate_conn.close()
    return True
  except Exception:  # pylint: disable=broad-except
    return False


class VTGateTestLogger(vtdb_logger.VtdbLogger):

  def __init__(self):
    self._integrity_error_count = 0

  def integrity_error(self, e):
    self._integrity_error_count += 1

  def get_integrity_error_count(self):
    return self._integrity_error_count


DML_KEYWORDS = ['insert', 'update', 'delete']


class TestExceptionLogging(BaseTestCase):

  def setUp(self):
    super(TestExceptionLogging, self).setUp()
    self.shard_index = 1
    self.keyrange = keyrange.KeyRange(SHARD_NAMES[self.shard_index])
    self.master_tablet = shard_1_master
    self.replica_tablet = shard_1_replica1
    vtdb_logger.register_vtdb_logger(VTGateTestLogger())
    self.logger = vtdb_logger.get_logger()

  def test_integrity_error_logging(self):
    vtgate_conn = get_connection()

    vtgate_conn.begin()
    vtgate_conn._execute(
        'delete from vt_a', {},
        tablet_type='master', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange])
    vtgate_conn.commit()

    keyspace_id = SHARD_KID_MAP[SHARD_NAMES[self.shard_index]][0]

    old_error_count = self.logger.get_integrity_error_count()
    try:
      vtgate_conn.begin()
      vtgate_conn._execute(
          'insert into vt_a (eid, id, keyspace_id) '
          'values (:eid, :id, :keyspace_id)',
          {'eid': 1, 'id': 1, 'keyspace_id': keyspace_id},
          tablet_type='master', keyspace_name=KEYSPACE_NAME,
          keyspace_ids=[pack_kid(keyspace_id)])
      vtgate_conn._execute(
          'insert into vt_a (eid, id, keyspace_id) '
          'values (:eid, :id, :keyspace_id)',
          {'eid': 1, 'id': 1, 'keyspace_id': keyspace_id},
          tablet_type='master', keyspace_name=KEYSPACE_NAME,
          keyspace_ids=[pack_kid(keyspace_id)])
      vtgate_conn.commit()
    except dbexceptions.IntegrityError as e:
      parts = str(e).split(',')
      exc_msg = parts[0]
      for kw in DML_KEYWORDS:
        if kw in exc_msg:
          self.fail("IntegrityError shouldn't contain the query %s" % exc_msg)
    except Exception as e:  # pylint: disable=broad-except
      self.fail('Expected IntegrityError to be raised, raised %s' % str(e))
    finally:
      vtgate_conn.rollback()
    # The underlying execute is expected to catch and log the integrity error.
    self.assertEqual(self.logger.get_integrity_error_count(), old_error_count+1)


if __name__ == '__main__':
  utils.main()
