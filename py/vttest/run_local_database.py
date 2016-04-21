#!/usr/bin/env python

"""Command-line tool for starting a local Vitess database for testing.

USAGE:

  $ run_local_database --port 12345 \
    --topology test_keyspace/-80:test_keyspace_0,test_keyspace/80-:test_keyspace_1 \
    --schema_dir /path/to/schema/dir

It will run the tool, logging to stderr. On stdout, a small json structure
can be waited on and then parsed by the caller to figure out how to reach
the vtgate process.

Once done with the test, send an empty line to this process for it to clean-up,
and then just wait for it to exit.

"""

import json
import logging
import optparse
import os
import re
import sys

from vttest import environment
from vttest import local_database
from vttest import mysql_flavor
from vttest import vt_processes
from vttest import init_data_options

shard_exp = re.compile(r'(.+)/(.+):(.+)')


def main(cmdline_options):
  shards = []

  for shard in cmdline_options.topology.split(','):
    m = shard_exp.match(shard)
    if m:
      shards.append(
          vt_processes.ShardInfo(m.group(1), m.group(2), m.group(3)))
    else:
      sys.stderr.write('invalid --shard flag format: %s\n' % shard)
      sys.exit(1)

  environment.base_port = cmdline_options.port
  init_data_opts = None
  if cmdline_options.initialize_with_random_data:
    init_data_opts = init_data_options.InitDataOptions()
    init_data_opts.rng_seed = cmdline_options.rng_seed
    init_data_opts.min_table_shard_size = cmdline_options.min_table_shard_size
    init_data_opts.max_table_shard_size = cmdline_options.max_table_shard_size
    init_data_opts.null_probability = cmdline_options.null_probability
    with local_database.LocalDatabase(
        shards,
        cmdline_options.schema_dir,
        cmdline_options.vschema,
        cmdline_options.mysql_only,
        init_data_opts,
        web_dir=cmdline_options.web_dir) as local_db:
      print json.dumps(local_db.config())
      sys.stdout.flush()
      try:
        raw_input()
      except EOFError:
        sys.stderr.write(
            'WARNING: %s: No empty line was received on stdin.'
            ' Instead, stdin was closed and the cluster will be shut down now.'
            ' Make sure to send the empty line instead to proactively shutdown'
            ' the local cluster. For example, did you forget the shutdown in'
            ' your test\'s tearDown()?\n' % os.path.basename(__file__))

if __name__ == '__main__':

  parser = optparse.OptionParser()
  parser.add_option(
      '-p', '--port', type='int',
      help='Port to use for vtcombo. If this is 0, a random port '
      'will be chosen.')
  parser.add_option(
      '-t', '--topology',
      help='Define which shards exist in the test topology in the'
      ' form <keyspace>/<shardrange>:<dbname>,... The dbname'
      ' must be unique among all shards, since they share'
      ' a MySQL instance in the test environment.')
  parser.add_option(
      '-s', '--schema_dir',
      help='Directory for initial schema files. Within this dir,'
      ' there should be a subdir for each keyspace. Within'
      ' each keyspace dir, each file is executed as SQL'
      ' after the database is created on each shard.'
      ' If the directory contains a vschema.json file, it'
      ' will be used as the vschema for the V3 API.')
  parser.add_option(
      '-e', '--vschema',
      help='If this file is specified, it will be used'
      ' as the vschema for the V3 API.')
  parser.add_option(
      '-m', '--mysql_only', action='store_true',
      help='If this flag is set only mysql is initialized.'
      ' The rest of the vitess components are not started.'
      ' Also, the output specifies the mysql unix socket'
      ' instead of the vtgate port.')
  parser.add_option(
      '-r', '--initialize_with_random_data', action='store_true',
      help='If this flag is each table-shard will be initialized'
      ' with random data. See also the "rng_seed" and "min_shard_size"'
      ' and "max_shard_size" flags.')
  parser.add_option(
      '-d', '--rng_seed', type='int', default=123,
      help='The random number generator seed to use when initializing'
      ' with random data (see also --initialize_with_random_data).'
      ' Multiple runs with the same seed will result with the same'
      ' initial data.')
  parser.add_option(
      '-x', '--min_table_shard_size', type='int', default=1000,
      help='The minimum number of initial rows in a table shard. Ignored if'
      '--initialize_with_random_data is false. The actual number is chosen'
      ' randomly.')
  parser.add_option(
      '-y', '--max_table_shard_size', type='int', default=10000,
      help='The maximum number of initial rows in a table shard. Ignored if'
      '--initialize_with_random_data is false. The actual number is chosen'
      ' randomly')
  parser.add_option(
      '-n', '--null_probability', type='float', default=0.1,
      help='The probability to initialize a field with "NULL" '
      ' if --initialize_with_random_data is true. Only applies to fields'
      ' that can contain NULL values.')
  parser.add_option(
      '-w', '--web_dir',
      help='location of the vtctld web server files.')
  parser.add_option(
      '-v', '--verbose', action='store_true',
      help='Display extra error messages.')
  (options, args) = parser.parse_args()
  if options.verbose:
    logging.getLogger().setLevel(logging.DEBUG)

  # This will set the flavor based on the MYSQL_FLAVOR env var,
  # or default to MariaDB.
  mysql_flavor.set_mysql_flavor(None)

  main(options)
