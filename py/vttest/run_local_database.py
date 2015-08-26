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
import re
import sys

from vttest import environment
from vttest import local_database
from vttest import vt_processes

shard_exp = re.compile(r'(.+)/(.+):(.+)')


def main(port, topology, schema_dir):
  shards = []

  for shard in topology.split(','):
    m = shard_exp.match(shard)
    if m:
      shards.append(
          vt_processes.ShardInfo(m.group(1), m.group(2), m.group(3)))
    else:
      sys.stderr.write('invalid --shard flag format: %s\n' % shard)
      sys.exit(1)

  environment.base_port = port
  with local_database.LocalDatabase(shards, schema_dir) as local_db:
    print json.dumps(local_db.config())
    sys.stdout.flush()
    raw_input()

if __name__ == '__main__':

  parser = optparse.OptionParser()
  parser.add_option('-p', '--port', type='int',
                    help='Port to use for vtgate. If this is 0, a random port'
                         ' will be chosen.')
  parser.add_option('-t', '--topology',
                    help='Define which shards exist in the test topology in the'                         ' form <keyspace>/<shardrange>:<dbname>,... The dbname'
                         ' must be unique among all shards, since they share'
                         ' a MySQL instance in the test environment.')
  parser.add_option('-s', '--schema_dir',
                    help='Directory for initial schema files. Within this dir,'
                         ' there should be a subdir for each keyspace. Within'
                         ' each keyspace dir, each file is executed as SQL'
                         ' after the database is created on each shard.')
  parser.add_option('-v', '--verbose', action='store_true',
                    help='Display extra error messages.')
  (options, args) = parser.parse_args()
  if options.verbose:
    logging.getLogger().setLevel(logging.DEBUG)

  main(options.port, options.topology, options.schema_dir)
