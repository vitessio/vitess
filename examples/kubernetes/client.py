#!/usr/bin/env python

import argparse

from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import vtgatev2
from vtdb import vtgate_cursor

# Constants and params
UNSHARDED = [keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)]
cursorclass = vtgate_cursor.VTGateCursor

# Parse args
parser = argparse.ArgumentParser()
parser.add_argument('--server', dest='server', default='localhost:15001')
parser.add_argument('--timeout', dest='timeout', type=float, default='10.0')
args = parser.parse_args()

vtgate_addrs = {"_vt": [args.server]}

# Connect
conn = vtgatev2.connect(vtgate_addrs, args.timeout)

# Insert something.
print('Inserting into master...')
cursor = conn.cursor(cursorclass, conn, 'test_keyspace', 'master',
                     keyranges=UNSHARDED, writable=True)
cursor.begin()
cursor.execute(
    "INSERT INTO test_table (msg) VALUES (%(msg)s)",
    {'msg': 'V is for speed'})
cursor.commit()


# Read it back from the master.
print('Reading from master...')
cursor.execute('SELECT * FROM test_table', {})
for row in cursor.fetchall():
  print(row)

cursor.close()

# Read from a replica.
# Note that this may be behind master due to replication lag.
print('Reading from replica...')
cursor = conn.cursor(cursorclass, conn, 'test_keyspace', 'replica',
                     keyranges=UNSHARDED)
cursor.execute('SELECT * FROM test_table', {})
for row in cursor.fetchall():
  print(row)
cursor.close()

# Clean up
conn.close()
