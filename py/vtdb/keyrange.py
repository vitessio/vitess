# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from vtdb import dbexceptions

# This module computes task map and query where clause and
# bind_vars for distrubuting the workload of streaming queries.


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
      kr_chunks.append(hex(kr))
    kr_chunks[-1] = ''
    self.keyrange_list = [(kr_chunks[i], kr_chunks[i+1]) for i in xrange(len(kr_chunks) - 1)]


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
def _true_keyspace_id_value(kr_value):
  if kr_value == '':
    return None
  if kr_value.startswith('0x'):
    kr_value = kr_value.split('0x')[1]
  true_hex_val = kr_value + (16-len(kr_value))*'0'
  return int(true_hex_val, base=16)


# Compute the where clause and bind_vars for a given keyrange.
def create_where_clause_for_keyrange(keyrange, col_name='keyspace_id'):
  kr_min = None
  kr_max = None
  if isinstance(keyrange, str):
    keyrange = keyrange.split('-')

  if (isinstance(keyrange, tuple) or isinstance(keyrange, list)) and len(keyrange) == 2:
    kr_min = _true_keyspace_id_value(keyrange[0])
    kr_max = _true_keyspace_id_value(keyrange[1])
  else:
    raise dbexceptions.ProgrammingError("keyrange must be a list or tuple or a '-' separated str %s" % keyrange)	

  where_clause = ''
  bind_vars = {}
  i = 0
  if kr_min is not None:
    bind_name = "%s%d" % (col_name, i)
    where_clause = "%s >= " % col_name + "%(" + bind_name + ")s"
    i += 1
    bind_vars[bind_name] = kr_min 
  if kr_max is not None:
    if where_clause != '':
      where_clause += ' AND '
    bind_name = "%s%d" % (col_name, i)
    where_clause += "%s < " % col_name + "%(" + bind_name + ")s"
    bind_vars[bind_name] = kr_max 
  return where_clause, bind_vars
