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
from vtdb import vtgatev2

# Constants and params
UNSHARDED = [keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)]

# Parse args
parser = argparse.ArgumentParser()
parser.add_argument('--server', dest='server', default='localhost:15001')
parser.add_argument('--timeout', dest='timeout', type=float, default='10.0')
args = parser.parse_args()

vtgate_addrs = {'vt': [args.server]}

# Connect
conn = vtgatev2.connect(vtgate_addrs, args.timeout)

# Insert something.
print 'Inserting into master...'
cursor = conn.cursor('test_keyspace', 'master',
                     keyranges=UNSHARDED, writable=True)
cursor.begin()
cursor.execute(
    'INSERT INTO test_table (msg) VALUES (%(msg)s)',
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
cursor = conn.cursor('test_keyspace', 'replica',
                     keyranges=UNSHARDED)
cursor.execute('SELECT * FROM test_table', {})
for row in cursor.fetchall():
  print row
cursor.close()

# Clean up
conn.close()
