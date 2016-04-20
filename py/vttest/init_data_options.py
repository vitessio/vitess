"""Stores options used for initializing the database with randomized data.

The options stored correspond to command line flags. See run_local_database.py
for more details on each option.
"""


class InitDataOptions(object):
  valid_attrs = set([
      'rng_seed',
      'min_table_shard_size',
      'max_table_shard_size',
      'null_probability',
      ])

  def __setattr__(self, name, value):
    if name not in self.valid_attrs:
      raise Exception(
          'InitDataOptions: unsupported attribute: %s' % name)
    self.__dict__[name] = value
