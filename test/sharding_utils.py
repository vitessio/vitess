#!/usr/bin/env python

"""Sharding utils."""


def get_shard_name(shard, num_shards):
  """Returns an appropriate shard name, as a string.

  A single shard name is simply 0; otherwise it will attempt to split up 0x100
  into multiple shards.  For example, in a two sharded keyspace, shard 0 is
  -80, shard 1 is 80-.  This function currently only applies to sharding setups
  where the shard count is 256 or less, and all shards are equal width.

  Args:
    shard: The integer shard index (zero based)
    num_shards: Total number of shards (int)

  Returns:
    The shard name as a string.
  """

  if num_shards == 1:
    return '0'

  shard_width = int(0x100 / num_shards)

  if shard == 0:
    return '-%02x' % shard_width
  elif shard == num_shards - 1:
    return '%02x-' % (shard * shard_width)
  else:
    return '%02x-%02x' % (shard * shard_width, (shard + 1) * shard_width)


def get_shard_names(num_shards):
  """Create a generator of shard names.

  Args:
    num_shards: Total number of shards (int)

  Returns:
    The shard name generator.
  """
  return (get_shard_name(x, num_shards) for x in range(num_shards))
