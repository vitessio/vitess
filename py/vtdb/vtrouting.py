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


"""Library for computing the parallization and routing for streaming queries.

The client computes the keyranges to parallelize over
based on the desired parallization and shard count.
Please note that the shard count maybe greater than equal
to actual shards. It is suggested to keep this value
at the upsharded count in case a resharding event is planned.
These keyranges can then be used to derived the routing
information. The routing information object is supposed to
be opaque to the client but VTGate uses it to route queries.
It is also used to compute the associated where clause to be added
to the query, wich is needed to prevent returning duplicate result
from two parallelized queries running over the same shard.

API usage -

task_map = vtrouting.create_parallel_task_keyrange_map(num_tasks, shard_count)
keyrange_list = task_map.keyrange_list

for db_keyrange in keyrange_list:
  vt_routing_info = vtrouting.create_vt_routing_info(db_keyrange, 'ruser')

During query construction -

where_clause, bind_vars = vt_routing_info.update_where_clause(
           where_clause, bind_vars)
"""

from vtdb import dbexceptions
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import topology


class TaskKeyrangeMap(object):
  """Class for computing keyranges for parallel tasks.

  Attributes:
   num_tasks: number of parallel queries.
  """

  def __init__(self, num_tasks):
    self.num_tasks = num_tasks
    self.keyrange_list = []
    self.compute_kr_list()

  def compute_kr_list(self):
    """compute the keyrange list for parallel queries.
    """
    kr_chunks = []
    # we only support 256 shards for now
    min_key_hex = int('00', base=16)
    max_key_hex = int('100', base=16)
    kr = min_key_hex
    span = (max_key_hex - min_key_hex)/self.num_tasks
    kr_chunks.append('')
    for i in range(self.num_tasks):
      kr += span
      kr_chunks.append('%.2x' % kr)
    kr_chunks[-1] = ''
    for i in range(len(kr_chunks) - 1):
      start = kr_chunks[i]
      end = kr_chunks[i+1]
      kr = keyrange.KeyRange((start, end,))
      self.keyrange_list.append(str(kr))


class VTRoutingInfo(object):
  """Class to encompass the keyrange and compute the where clause with the same.

  Attributes:
    db_keyrange: the keyrange to route the query to.
    extra_where_clause: where clause with the sharding key for the keyspace.
    extra_bind_vars: bind var dictionary with the sharding key for the keyspace.
  """

  def __init__(self, db_keyrange, where_clause, bind_vars):
    self.db_keyrange = db_keyrange
    self.extra_where_clause = where_clause
    self.extra_bind_vars = bind_vars

  def update_where_clause(self, where_clause, bind_vars):
    """Updates the existing where clause of the query with the routing info.

    Args:
      where_clause: the where_clause of the query being constructed.
      bind_vars: bind_vars of the query being constructed.

    Returns:
      updated where_clause and bind_vars with the sharding key
      of the keyspace and the db_keyrange.
      Example -
      Assuming sharding key of keyspace 'test_keyspace' is
      'keyspace_id'.
      returned_where_clause = where_clause AND keyspace_id <= <db_keyrange>
      returned_bind_vars = {'keyspace_id': <db_keyrange>}
    """
    if self.extra_where_clause:
      if where_clause:
        where_clause += ' AND ' + self.extra_where_clause
      else:
        where_clause = self.extra_where_clause
    if self.extra_bind_vars:
      bind_vars.update(self.extra_bind_vars)
    return where_clause, bind_vars


def create_parallel_task_keyrange_map(num_tasks, shard_count):
  """Compute the task map for a streaming query.

  Args:
    num_tasks: desired parallelization of queries.
    shard_count: configurable global shard count for keyspace.
    This can be greater than actual shards and should be set to
    the desired upsharded value before a resharding event.
    This is so that the keyrange->shard map for an inflight
    query is always unique.

  Returns:
    TaskKeyrangeMap object
  """
  if num_tasks % shard_count != 0:
    raise dbexceptions.ProgrammingError(
        'tasks %d should be multiple of shard_count %d' % (num_tasks,
                                                           shard_count))
  return TaskKeyrangeMap(num_tasks)


def create_vt_routing_info(key_range, keyspace_name):
  """Creates VTRoutingInfo object for the given key_range in keyspace.

  Args:
    key_range: keyrange for query routing.
    keyspace_name: target keyspace for the query. This is used to derive
    sharding column type and column name information for the where clause.

  Returns:
    VTRoutingInfo object.
  """
  col_name, col_type = topology.get_sharding_col(keyspace_name)
  if not col_name or col_type == keyrange_constants.KIT_UNSET:
    return VTRoutingInfo(key_range, '', {})
  where_clause, bind_vars = _create_where_clause_for_keyrange(key_range,
                                                              col_name,
                                                              col_type)
  return VTRoutingInfo(key_range, where_clause, bind_vars)


def _true_int_kr_value(kr_value):
  """This returns the true value of keyrange for comparison with keyspace_id.

  We abbreviate the keyranges for ease of use.
  To obtain true value for comparison with keyspace id,
  create true hex value for that keyrange by right padding and conversion.
  Args:
    kr_value: short keyranges as used by vitess.
  Returns:
    complete hex value of keyrange.
  """
  if kr_value == '':
    return None
  kr_value += (16-len(kr_value))*'0'
  if not kr_value.startswith('0x'):
    kr_value = '0x' + kr_value
  return int(kr_value, base=16)


def _create_where_clause_for_keyrange(
    key_range, keyspace_col_name='keyspace_id',
    keyspace_col_type=keyrange_constants.KIT_UINT64):
  """Compute the where clause and bind_vars for a given keyrange.

  Args:
    key_range: keyrange for the query.
    keyspace_col_name: keyspace column name of keyspace.
    keyspace_col_type: keyspace column type of keyspace.
  Returns:
    where clause for the keyrange.
  """

  if isinstance(key_range, str):
    # If the key_range is for unsharded db, there is no
    # where clause to add to or bind_vars to add to.
    if key_range == keyrange_constants.NON_PARTIAL_KEYRANGE:
      return '', {}
    key_range = key_range.split('-')

  if (not isinstance(key_range, tuple) and not isinstance(key_range, list) or
      len(key_range) != 2):
    raise dbexceptions.ProgrammingError(
        'key_range must be list or tuple or \'-\' separated str %s' % key_range)

  if keyspace_col_type == keyrange_constants.KIT_UINT64:
    return _create_where_clause_for_int_keyspace(key_range, keyspace_col_name)
  elif keyspace_col_type == keyrange_constants.KIT_BYTES:
    return _create_where_clause_for_str_keyspace(key_range, keyspace_col_name)
  else:
    raise dbexceptions.ProgrammingError(
        'Illegal type for keyspace_col_type %d' % keyspace_col_type)


def _create_where_clause_for_str_keyspace(key_range, keyspace_col_name):
  """This creates the where clause and bind_vars if keyspace_id col is a str.

  The comparison is done using mysql hex function and byte level comparison
  with the key_range values.

  Args:
    key_range: keyrange for the query.
    keyspace_col_name: keyspace column name for the keyspace.

  Returns:
    This returns the where clause when the keyspace column type is string.
  """
  kr_min = key_range[0].strip()
  kr_max = key_range[1].strip()

  where_clause = ''
  bind_vars = {}
  i = 0
  if kr_min != keyrange_constants.MIN_KEY:
    bind_name = '%s%d' % (keyspace_col_name, i)
    where_clause = 'hex(%s) >= ' % keyspace_col_name + '%(' + bind_name + ')s'
    i += 1
    bind_vars[bind_name] = kr_min
  if kr_max != keyrange_constants.MAX_KEY:
    if where_clause:
      where_clause += ' AND '
    bind_name = '%s%d' % (keyspace_col_name, i)
    where_clause += 'hex(%s) < ' % keyspace_col_name + '%(' + bind_name + ')s'
    bind_vars[bind_name] = kr_max
  return where_clause, bind_vars


def _create_where_clause_for_int_keyspace(key_range, keyspace_col_name):
  """This creates the where clause and bind_vars if keyspace_id col is a int.

  The comparison is done using numeric comparison on the int values hence
  the true 64-bit int values are generated for the key_range in the bind_vars.

  Args:
    key_range: keyrange for the query.
    keyspace_col_name: keyspace column name for the keyspace.

  Returns:
    This returns the where clause when the keyspace column type is uint64.
  """
  kr_min = _true_int_kr_value(key_range[0])
  kr_max = _true_int_kr_value(key_range[1])

  where_clause = ''
  bind_vars = {}
  i = 0
  if kr_min is not None:
    bind_name = '%s%d' % (keyspace_col_name, i)
    where_clause = '%s >= ' % keyspace_col_name + '%(' + bind_name + ')s'
    i += 1
    bind_vars[bind_name] = kr_min
  if kr_max is not None:
    if where_clause:
      where_clause += ' AND '
    bind_name = '%s%d' % (keyspace_col_name, i)
    where_clause += '%s < ' % keyspace_col_name + '%(' + bind_name + ')s'
    bind_vars[bind_name] = kr_max
  return where_clause, bind_vars
