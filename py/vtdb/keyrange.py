# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from vtdb import dbexceptions

# This module computes task map and query where clause and
# bind_vars for distrubuting the workload of streaming queries.


class KeyRange(object):
  kr_min = None
  kr_max = None

  def __init__(self, kr):
    if isinstance(kr, str):
      kr = kr.split('-')
    if not isinstance(kr, tuple) and not isinstance(kr, list) or len(kr) != 2:
      raise dbexceptions.ProgrammingError("keyrange must be a list or tuple or a '-' separated str %s" % keyrange)
    self.kr_min = kr[0].strip()
    self.kr_max = kr[1].strip()

  def __str__(self):
    return '%s-%s' % (self.kr_min, self.kr_max)


class StreamingTaskMap(object):
  keyrange_list = None

  def __init__(self, num_tasks):
    self.num_tasks = num_tasks

  def compute_kr_list(self):
    self.keyrange_list = []
    kr_chunks = []
    min_key_hex = int("00", base=16)
    max_key_hex = int("100", base=16)
    kr = min_key_hex
    kr_chunks.append('')
    span = (max_key_hex - min_key_hex)/self.num_tasks
    for i in xrange(self.num_tasks):
      kr += span
      #kr_chunks.append(hex(kr).split('0x')[1])
      kr_chunks.append('%x' % kr)
    kr_chunks[-1] = ''
    self.keyrange_list = [str(KeyRange((kr_chunks[i], kr_chunks[i+1],))) for i in xrange(len(kr_chunks) - 1)]


# Compute the task map for a streaming query.
# global_shard_count is read from config, using it as a param for simplicity.
def create_streaming_task_map(num_tasks, global_shard_count):
  # global_shard_count is a configurable value controlled for resharding.
  if num_tasks < global_shard_count:
    raise dbexceptions.ProgrammingError("Tasks %d cannot be less than number of shards %d" % (num_tasks, global_shard_count))

  stm = StreamingTaskMap(num_tasks)
  stm.compute_kr_list()
  return stm


# We abbreviate the keyranges for ease of use.
# To obtain true value for comparison with keyspace id,
# create true hex value for that keyrange by right padding and conversion.
def _true_int_kr_value(kr_value):
  if kr_value == '':
    return None
  kr_value = kr_value + (16-len(kr_value))*'0'
  if not kr_value.startswith('0x'):
    kr_value = '0x' + kr_value
  return int(kr_value, base=16)


MIN_KEY = ''
MAX_KEY = ''

KIT_UNSET = ""
KIT_UINT64 = "uint64"
KIT_BYTES = "bytes"


# Compute the where clause and bind_vars for a given keyrange.
def create_where_clause_for_keyrange(keyrange, keyspace_col_name='keyspace_id', keyspace_col_type=KIT_UINT64):
  if isinstance(keyrange, str):
    keyrange = keyrange.split('-')

  if not isinstance(keyrange, tuple) and not isinstance(keyrange, list) or len(keyrange) != 2:
    raise dbexceptions.ProgrammingError("keyrange must be a list or tuple or a '-' separated str %s" % keyrange)

  if keyspace_col_type == KIT_UINT64:
    return _create_where_clause_for_int_keyspace(keyrange, keyspace_col_name)
  elif keyspace_col_type == KIT_BYTES:
    return _create_where_clause_for_str_keyspace(keyrange, keyspace_col_name)
  else:
    raise dbexceptions.ProgrammingError("Illegal type for keyspace_col_type %d" % keyspace_col_type)

# This creates the where clause and bind_vars if keyspace_id col is a str.
# The comparison is done using mysql hex function and byte level comparison
# with the keyrange values.
def _create_where_clause_for_str_keyspace(keyrange, keyspace_col_name):
  kr_min = keyrange[0].strip()
  kr_max = keyrange[1].strip()

  where_clause = ''
  bind_vars = {}
  i = 0
  if kr_min != MIN_KEY:
    bind_name = "%s%d" % (keyspace_col_name, i)
    where_clause = "hex(%s) >= " % keyspace_col_name + "%(" + bind_name + ")s"
    i += 1
    bind_vars[bind_name] = kr_min
  if kr_max != MAX_KEY:
    if where_clause != '':
      where_clause += ' AND '
    bind_name = "%s%d" % (keyspace_col_name, i)
    where_clause += "hex(%s) < " % keyspace_col_name + "%(" + bind_name + ")s"
    bind_vars[bind_name] = kr_max
  return where_clause, bind_vars


# This creates the where clause and bind_vars if keyspace_id col is a int.
# The comparison is done using numeric comparison on the int values hence
# the true 64 bit int values are generated for the keyrange values in the bind_vars.
def _create_where_clause_for_int_keyspace(keyrange, keyspace_col_name):
  kr_min = _true_int_kr_value(keyrange[0])
  kr_max = _true_int_kr_value(keyrange[1])

  where_clause = ''
  bind_vars = {}
  i = 0
  if kr_min is not None:
    bind_name = "%s%d" % (keyspace_col_name, i)
    where_clause = "%s >= " % keyspace_col_name + "%(" + bind_name + ")s"
    i += 1
    bind_vars[bind_name] = kr_min
  if kr_max is not None:
    if where_clause != '':
      where_clause += ' AND '
    bind_name = "%s%d" % (keyspace_col_name, i)
    where_clause += "%s < " % keyspace_col_name + "%(" + bind_name + ")s"
    bind_vars[bind_name] = kr_max
  return where_clause, bind_vars
