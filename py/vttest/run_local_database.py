#!/usr/bin/env python

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


r"""Command-line tool for starting a local Vitess database for testing.

USAGE:

  $ run_local_database --port 12345 \
    --proto_topo <vttest proto as string> \
    --schema_dir /path/to/schema/dir

It will run the tool, logging to stderr. On stdout, a small json structure
can be waited on and then parsed by the caller to figure out how to reach
the vtgate process.

As an alternative to using proto_topo, a local instance can be started by using
additional flags, such as:

  $ run_local_database --port 12345 \
    --schema_dir /path/to/schema/dir \
    --cells cell1,cell2 --keyspaces ks1,ks2 \
    --num_shards 1,2

This will create an instance with two keyspaces in two cells, one with a single
shard and another with two shards.

Once done with the test, send an empty line to this process for it to clean-up,
and then just wait for it to exit.

"""

import json
import logging
import optparse
import os
import sys


from google.protobuf import text_format

from vtproto import vttest_pb2
from vtdb import prefer_vtroot_imports  # pylint: disable=unused-import
from vttest import environment
from vttest import init_data_options
from vttest import local_database
from vttest import mysql_flavor
from vttest import sharding_utils


def main(cmdline_options):
  topology = vttest_pb2.VTTestTopology()
  if cmdline_options.proto_topo:
    # Text-encoded proto topology object, just parse it.
    topology = text_format.Parse(cmdline_options.proto_topo, topology)
    if not topology.cells:
      topology.cells.append('test')
  else:
    cells = []
    keyspaces = []
    shard_counts = []
    if cmdline_options.cells:
      cells = cmdline_options.cells.split(',')
    if cmdline_options.keyspaces:
      keyspaces = cmdline_options.keyspaces.split(',')
    if cmdline_options.num_shards:
      shard_counts = [int(x) for x in cmdline_options.num_shards.split(',')]

    for cell in cells:
      topology.cells.append(cell)
    for keyspace, num_shards in zip(keyspaces, shard_counts):
      ks = topology.keyspaces.add(name=keyspace)
      for shard in sharding_utils.get_shard_names(num_shards):
        ks.shards.add(name=shard)
      ks.replica_count = cmdline_options.replica_count
      ks.rdonly_count = cmdline_options.rdonly_count

  environment.base_port = cmdline_options.port

  init_data_opts = None
  if cmdline_options.initialize_with_random_data:
    init_data_opts = init_data_options.InitDataOptions()
    init_data_opts.rng_seed = cmdline_options.rng_seed
    init_data_opts.min_table_shard_size = cmdline_options.min_table_shard_size
    init_data_opts.max_table_shard_size = cmdline_options.max_table_shard_size
    init_data_opts.null_probability = cmdline_options.null_probability

  extra_my_cnf = os.path.join(os.environ['VTTOP'], 'config/mycnf/vtcombo.cnf')
  if cmdline_options.extra_my_cnf:
    extra_my_cnf += ':' + cmdline_options.extra_my_cnf

  with local_database.LocalDatabase(
      topology,
      cmdline_options.schema_dir,
      cmdline_options.mysql_only,
      init_data_opts,
      web_dir=cmdline_options.web_dir,
      web_dir2=cmdline_options.web_dir2,
      default_schema_dir=cmdline_options.default_schema_dir,
      extra_my_cnf=extra_my_cnf,
      charset=cmdline_options.charset,
      snapshot_file=cmdline_options.snapshot_file) as local_db:
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
      '-o', '--proto_topo',
      help='Define the fake cluster topology as a compact text format encoded'
      ' vttest proto. See vttest.proto for more information.')
  parser.add_option(
      '-s', '--schema_dir',
      help='Directory for initial schema files. Within this dir,'
      ' there should be a subdir for each keyspace. Within'
      ' each keyspace dir, each file is executed as SQL'
      ' after the database is created on each shard.'
      ' If the directory contains a vschema.json file, it'
      ' will be used as the vschema for the V3 API.')
  parser.add_option(
      '-e', '--default_schema_dir',
      help='Default directory for initial schema files. If no schema is found'
      ' in schema_dir, default to this location.')
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
      '--web_dir2',
      help='location of the vtctld2 web server files.')
  parser.add_option(
      '-f', '--extra_my_cnf',
      help='extra files to add to the config, separated by ":"')
  parser.add_option(
      '-v', '--verbose', action='store_true',
      help='Display extra error messages.')
  parser.add_option('-c', '--cells', default='test',
                    help='Comma separated list of cells')
  parser.add_option('-k', '--keyspaces', default='test_keyspace',
                    help='Comma separated list of keyspaces')
  parser.add_option('--num_shards', default='2',
                    help='Comma separated shard count (one per keyspace)')
  parser.add_option('--replica_count', type='int', default=2,
                    help='Replica tablets per shard (includes master)')
  parser.add_option('--rdonly_count', type='int', default=1,
                    help='Rdonly tablets per shard')
  parser.add_option('--charset', default='utf8', help='MySQL charset')
  parser.add_option(
      '--snapshot_file', default=None, help='A MySQL DB snapshot file')
  (options, args) = parser.parse_args()
  if options.verbose:
    logging.getLogger().setLevel(logging.DEBUG)

  # This will set the flavor based on the MYSQL_FLAVOR env var,
  # or default to MySQL56.
  mysql_flavor.set_mysql_flavor(None)

  main(options)
