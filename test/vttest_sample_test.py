#!/usr/bin/env python

# Copyright 2015 Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""Sample test for vttest framework.

This sample test demonstrates how to setup and teardown a test
database with the associated Vitess processes. It is meant to be used
as a template for unit tests, by developers writing applications
on top of Vitess. The recommended workflow is to have the schema for the
database checked into a source control system, along with the application
layer code. Then unit tests can launch a test cluster for unit tests
(using local_database.py) pointing at that schema, and run all their unit
tests.

This unit test is written in python, but we don't depend on this at all.
We just execute py/vttest/local_database.py, as we would in any language.
Then we wait for the JSON config string, and we know the address of the
vtgate process. At the end of the test, sending a line to the underlying
process makes it clean up.
"""

import json
import os
import struct
import subprocess
import time
import urllib

import unittest

from google.protobuf import text_format

from vtproto import topodata_pb2
from vtproto import vttest_pb2

from vtdb import dbexceptions
from vtdb import proto3_encoding
from vtdb import vtgate_client
from vtdb import vtgate_cursor

import utils
import environment
from protocols_flavor import protocols_flavor


pack_kid = struct.Struct('!Q').pack


def get_keyspace_id(row_id):
  return row_id


class TestMysqlctl(unittest.TestCase):

  def test_standalone(self):
    """Sample test for run_local_database.py as a standalone process."""

    topology = vttest_pb2.VTTestTopology()
    keyspace = topology.keyspaces.add(name='test_keyspace')
    keyspace.replica_count = 2
    keyspace.rdonly_count = 1
    keyspace.shards.add(name='-80')
    keyspace.shards.add(name='80-')
    topology.keyspaces.add(name='redirect', served_from='test_keyspace')

    # launch a backend database based on the provided topology and schema
    port = environment.reserve_ports(1)
    args = [environment.run_local_database,
            '--port', str(port),
            '--proto_topo', text_format.MessageToString(topology,
                                                        as_one_line=True),
            '--schema_dir', os.path.join(environment.vttop, 'test',
                                         'vttest_schema'),
            '--web_dir', environment.vttop + '/web/vtctld',
           ]
    sp = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    config = json.loads(sp.stdout.readline())

    # gather the vars for the vtgate process
    url = 'http://localhost:%d/debug/vars' % config['port']
    f = urllib.urlopen(url)
    data = f.read()
    f.close()
    json_vars = json.loads(data)
    self.assertIn('vtcombo', json_vars['cmdline'][0])

    # build the vtcombo address and protocol
    protocol = protocols_flavor().vttest_protocol()
    if protocol == 'grpc':
      vtgate_addr = 'localhost:%d' % config['grpc_port']
    else:
      vtgate_addr = 'localhost:%d' % config['port']
    conn_timeout = 30.0
    utils.pause('Paused test after vtcombo was started.\n'
                'For manual testing, connect to vtgate at: %s '
                'using protocol: %s.\n'
                'Press enter to continue.' % (vtgate_addr, protocol))

    # Remember the current timestamp after we sleep for a bit, so we
    # can use it for UpdateStream later.
    time.sleep(2)
    before_insert = long(time.time())

    # Connect to vtgate.
    conn = vtgate_client.connect(protocol, vtgate_addr, conn_timeout)

    # Insert a row.
    row_id = 123
    keyspace_id = get_keyspace_id(row_id)
    cursor = conn.cursor(
        tablet_type='master', keyspace='test_keyspace',
        keyspace_ids=[pack_kid(keyspace_id)],
        writable=True)
    cursor.begin()
    insert = ('insert into test_table (id, msg, keyspace_id) values (:id, '
              ':msg, :keyspace_id)')
    bind_variables = {
        'id': row_id,
        'msg': 'test %s' % row_id,
        'keyspace_id': keyspace_id,
        }
    cursor.execute(insert, bind_variables)
    cursor.commit()

    # Read the row back.
    cursor.execute(
        'select * from test_table where id=:id', {'id': row_id})
    result = cursor.fetchall()
    self.assertEqual(result[0][1], 'test 123')

    # try to insert again, see if we get the right integrity error exception
    # (this is meant to test vtcombo properly returns exceptions, and to a
    # lesser extent that the python client converts it properly)
    cursor.begin()
    with self.assertRaises(dbexceptions.IntegrityError):
      cursor.execute(insert, bind_variables)
    cursor.rollback()

    # Insert a bunch of rows with long msg values.
    bind_variables['msg'] = 'x' * 64
    id_start = 1000
    rowcount = 500
    cursor.begin()
    for i in xrange(id_start, id_start+rowcount):
      bind_variables['id'] = i
      bind_variables['keyspace_id'] = get_keyspace_id(i)
      cursor.execute(insert, bind_variables)
    cursor.commit()
    cursor.close()

    # Try to fetch a large number of rows, from a rdonly
    # (more than one streaming result packet).
    stream_cursor = conn.cursor(
        tablet_type='rdonly', keyspace='test_keyspace',
        keyspace_ids=[pack_kid(keyspace_id)],
        cursorclass=vtgate_cursor.StreamVTGateCursor)
    stream_cursor.execute('select * from test_table where id >= :id_start',
                          {'id_start': id_start})
    self.assertEqual(rowcount, len(list(stream_cursor.fetchall())))
    stream_cursor.close()

    # try to read a row using the redirected keyspace, to a replica this time
    row_id = 123
    keyspace_id = get_keyspace_id(row_id)
    cursor = conn.cursor(
        tablet_type='replica', keyspace='redirect',
        keyspace_ids=[pack_kid(keyspace_id)])
    cursor.execute(
        'select * from test_table where id=:id', {'id': row_id})
    result = cursor.fetchall()
    self.assertEqual(result[0][1], 'test 123')
    cursor.close()

    # Try to get the update stream from the connection. This makes
    # sure that part works as well.
    count = 0
    for (event, _) in conn.update_stream(
        'test_keyspace', topodata_pb2.MASTER,
        timestamp=before_insert,
        shard='-80'):
      for statement in event.statements:
        if statement.table_name == 'test_table':
          count += 1
      if count == rowcount + 1:
        # We're getting the initial value, plus the 500 updates.
        break

    # Insert a sentinel value into the second shard.
    row_id = 0x8100000000000000
    keyspace_id = get_keyspace_id(row_id)
    cursor = conn.cursor(
        tablet_type='master', keyspace='test_keyspace',
        keyspace_ids=[pack_kid(keyspace_id)],
        writable=True)
    cursor.begin()
    bind_variables = {
        'id': row_id,
        'msg': 'test %s' % row_id,
        'keyspace_id': keyspace_id,
        }
    cursor.execute(insert, bind_variables)
    cursor.commit()
    cursor.close()

    # Try to connect to an update stream on the other shard.
    # We may get some random update stream events, but we should not get any
    # event that's related to the first shard. Only events related to
    # the Insert we just did.
    found = False
    for (event, _) in conn.update_stream(
        'test_keyspace', topodata_pb2.MASTER,
        timestamp=before_insert,
        shard='80-'):
      for statement in event.statements:
        self.assertEqual(statement.table_name, 'test_table')
        fields, rows = proto3_encoding.convert_stream_event_statement(statement)
        self.assertEqual(fields[0], 'id')
        self.assertEqual(rows[0][0], row_id)
        found = True
      if found:
        break

    # Clean up the connection
    conn.close()

    # Test we can connect to vtcombo for vtctl actions
    protocol = protocols_flavor().vtctl_python_client_protocol()
    if protocol == 'grpc':
      vtgate_addr = 'localhost:%d' % config['grpc_port']
    else:
      vtgate_addr = 'localhost:%d' % config['port']
    out, _ = utils.run(
        environment.binary_args('vtctlclient') +
        ['-vtctl_client_protocol', protocol,
         '-server', vtgate_addr,
         '-stderrthreshold', '0',
         'ListAllTablets', 'test',
        ], trap_output=True)
    num_master = 0
    num_replica = 0
    num_rdonly = 0
    num_dash_80 = 0
    num_80_dash = 0
    for line in out.splitlines():
      parts = line.split()
      self.assertEqual(parts[1], 'test_keyspace',
                       'invalid keyspace in line: %s' % line)
      if parts[3] == 'master':
        num_master += 1
      elif parts[3] == 'replica':
        num_replica += 1
      elif parts[3] == 'rdonly':
        num_rdonly += 1
      else:
        self.fail('invalid tablet type in line: %s' % line)
      if parts[2] == '-80':
        num_dash_80 += 1
      elif parts[2] == '80-':
        num_80_dash += 1
      else:
        self.fail('invalid shard name in line: %s' % line)
    self.assertEqual(num_master, 2)
    self.assertEqual(num_replica, 2)
    self.assertEqual(num_rdonly, 2)
    self.assertEqual(num_dash_80, 3)
    self.assertEqual(num_80_dash, 3)

    # and we're done, clean-up process
    sp.stdin.write('\n')
    sp.wait()

if __name__ == '__main__':
  utils.main()
