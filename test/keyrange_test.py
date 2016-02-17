#!/usr/bin/env python

import struct
import unittest

from vtdb import dbexceptions
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import vtrouting

# This unittest tests the computation of task map
# and where clauses for streaming queries.

pkid_pack = struct.Struct('!Q').pack
int_shard_kid_map = {
    '-10': [1, 100, 1000, 100000, 527875958493693904, 626750931627689502,
            345387386794260318, 332484755310826578],
    '10-20': [1842642426274125671, 1326307661227634652, 1761124146422844620,
              1661669973250483744],
    '20-30': [3361397649937244239, 3303511690915522723, 2444880764308344533,
              2973657788686139039],
    '30-40': [3821005920507858605, 4575089859165626432, 3607090456016432961,
              3979558375123453425],
    '40-50': [5129057445097465905, 5464969577815708398, 5190676584475132364,
              5762096070688827561],
    '50-60': [6419540613918919447, 6867152356089593986, 6601838130703675400,
              6132605084892127391],
    '60-70': [7251511061270371980, 7395364497868053835, 7814586147633440734,
              7968977924086033834],
    '70-80': [8653665459643609079, 8419099072545971426, 9020726671664230611,
              9064594986161620444],
    '80-90': [9767889778372766922, 9742070682920810358, 10296850775085416642,
              9537430901666854108],
    '90-a0': [10440455099304929791, 11454183276974683945, 11185910247776122031,
              10460396697869122981],
    'a0-b0': [11935085245138597119, 12115696589214223782, 12639360876311033978,
              12548906240535188165],
    'b0-c0': [13379616110062597001, 12826553979133932576, 13288572810772383281,
              13471801046560785347],
    'c0-d0': [14394342688314745188, 14639660031570920207, 14646353412066152016,
              14186650213447467187],
    'd0-e0': [15397348460895960623, 16014223083986915239, 15058390871463382185,
              15811857963302932363],
    'e0-f0': [17275711019497396001, 16979796627403646478, 16635982235308289704,
              16906674090344806032],
    'f0-': [18229242992218358675, 17623451135465171527, 18333015752598164958,
            17775908119782706671],
}

# str_shard_kid_map is derived from int_shard_kid_map
# by generating bin-packed strings from the int keyspace_id values.
str_shard_kid_map = dict(
    [(shard_name0, [pkid_pack(kid0) for kid0 in kid_list0])
     for shard_name0, kid_list0 in int_shard_kid_map.iteritems()])


class TestKeyRange(unittest.TestCase):

  def test_keyrange_correctness(self):
    kr = keyrange.KeyRange('')
    self.assertEqual(kr.Start, keyrange_constants.MIN_KEY)
    self.assertEqual(kr.End, keyrange_constants.MAX_KEY)
    self.assertEqual(str(kr), keyrange_constants.NON_PARTIAL_KEYRANGE)

    kr = keyrange.KeyRange('-')
    self.assertEqual(kr.Start, keyrange_constants.MIN_KEY)
    self.assertEqual(kr.End, keyrange_constants.MAX_KEY)
    self.assertEqual(str(kr), keyrange_constants.NON_PARTIAL_KEYRANGE)

    for kr_str in int_shard_kid_map:
      start_raw, end_raw = kr_str.split('-')
      kr = keyrange.KeyRange(kr_str)
      self.assertEqual(kr.Start, start_raw.strip().decode('hex'))
      self.assertEqual(kr.End, end_raw.strip().decode('hex'))
      self.assertEqual(str(kr), kr_str)

  def test_incorrect_tasks(self):
    global_shard_count = 16
    with self.assertRaises(dbexceptions.ProgrammingError):
      vtrouting.create_parallel_task_keyrange_map(4, global_shard_count)

  def test_keyranges_for_tasks(self):
    for shard_count in (16, 32, 64):
      for num_tasks in (shard_count, shard_count*2, shard_count*4):
        stm = vtrouting.create_parallel_task_keyrange_map(
            num_tasks, shard_count)
        self.assertEqual(len(stm.keyrange_list), num_tasks)

  # This tests that the where clause and bind_vars generated for each shard
  # against a few sample values where keyspace_id is an int column.
  def test_bind_values_for_int_keyspace(self):
    stm = vtrouting.create_parallel_task_keyrange_map(16, 16)
    for _, kr in enumerate(stm.keyrange_list):
      kr_parts = kr.split('-')
      where_clause, bind_vars = vtrouting._create_where_clause_for_keyrange(kr)
      if len(bind_vars.keys()) == 1:
        if kr_parts[0]:
          self.assertNotEqual(where_clause.find('>='), -1)
        else:
          self.assertNotEqual(where_clause.find('<'), -1)
      else:
        self.assertNotEqual(where_clause.find('>='), -1)
        self.assertNotEqual(where_clause.find('>='), -1)
        self.assertNotEqual(where_clause.find('AND'), -1)
      kid_list = int_shard_kid_map[kr]
      for keyspace_id in kid_list:
        if len(bind_vars.keys()) == 1:
          if kr_parts[0]:
            self.assertGreaterEqual(keyspace_id, bind_vars['keyspace_id0'])
          else:
            self.assertLess(keyspace_id, bind_vars['keyspace_id0'])
        else:
          self.assertGreaterEqual(keyspace_id, bind_vars['keyspace_id0'])
          self.assertLess(keyspace_id, bind_vars['keyspace_id1'])

  # This tests that the where clause and bind_vars generated for each shard
  # against a few sample values where keyspace_id is a str column.
  # mysql will use the hex function on string keyspace column
  # and use byte comparison. Since the exact function is not available,
  # the test emulates that by using keyspace_id.encode('hex').
  def test_bind_values_for_str_keyspace(self):
    stm = vtrouting.create_parallel_task_keyrange_map(16, 16)
    for _, kr in enumerate(stm.keyrange_list):
      kr_parts = kr.split('-')
      where_clause, bind_vars = vtrouting._create_where_clause_for_keyrange(
          kr, keyspace_col_type=keyrange_constants.KIT_BYTES)
      if len(bind_vars.keys()) == 1:
        if kr_parts[0]:
          self.assertNotEqual(where_clause.find('>='), -1)
        else:
          self.assertNotEqual(where_clause.find('<'), -1)
      else:
        self.assertNotEqual(where_clause.find('>='), -1)
        self.assertNotEqual(where_clause.find('>='), -1)
        self.assertNotEqual(where_clause.find('AND'), -1)
      kid_list = str_shard_kid_map[kr]
      for keyspace_id in kid_list:
        if len(bind_vars.keys()) == 1:
          if kr_parts[0]:
            self.assertGreaterEqual(
                keyspace_id.encode('hex'), bind_vars['keyspace_id0'])
          else:
            self.assertLess(
                keyspace_id.encode('hex'), bind_vars['keyspace_id0'])
        else:
          self.assertGreaterEqual(
              keyspace_id.encode('hex'), bind_vars['keyspace_id0'])
          self.assertLess(keyspace_id.encode('hex'), bind_vars['keyspace_id1'])

  def test_bind_values_for_unsharded_keyspace(self):
    stm = vtrouting.create_parallel_task_keyrange_map(1, 1)
    self.assertEqual(len(stm.keyrange_list), 1)
    where_clause, bind_vars = vtrouting._create_where_clause_for_keyrange(
        stm.keyrange_list[0])
    self.assertEqual(where_clause, '')
    self.assertEqual(bind_vars, {})

if __name__ == '__main__':
  unittest.main()
