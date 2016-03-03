#!/usr/bin/env python

"""Sample Vitess client in Python.

This is a sample for using the Python Vitess client with an unsharded keyspace.

Before running this, start up a local example cluster as described in the
README.md file.

Then run client.sh, which sets up PYTHONPATH before running client.py:
vitess/examples/local$ ./client.sh
"""

import argparse

from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import vtgate_client

# register the python gRPC client upon import
from vtdb import grpc_vtgate_client  # pylint: disable=unused-import

# Constants and params
UNSHARDED = [keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)]

# Parse args
parser = argparse.ArgumentParser()
parser.add_argument('--server', dest='server', default='localhost:15991')
parser.add_argument('--timeout', dest='timeout', type=float, default='10.0')
args = parser.parse_args()

# Connect
conn = vtgate_client.connect('grpc', args.server, args.timeout)

try:
  # Insert something.
  print 'Inserting into master...'
  cursor = conn.cursor(
      tablet_type='master', keyspace='test_keyspace',
      keyranges=UNSHARDED, writable=True)
  cursor.begin()
  cursor.execute(
      'INSERT INTO test_table (msg) VALUES (:msg)',
      {'msg': 'V is for speed'})
  cursor.commit()

  # Read it back from the master.
  print 'Reading from master...'
  cursor.execute('SELECT * FROM test_table', {})
  for row in cursor.fetchall():
    print row

  cursor.close()

  # Read from a replica.
  # Note that this may be behind master due to replication lag.
  print 'Reading from replica...'
  cursor = conn.cursor(
      tablet_type='replica', keyspace='test_keyspace', keyranges=UNSHARDED)
  cursor.execute('SELECT * FROM test_table', {})
  for row in cursor.fetchall():
    print row
  cursor.close()

finally:
  # Clean up
  conn.close()
