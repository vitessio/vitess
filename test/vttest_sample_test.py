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
import urllib

import unittest

from vtdb import vtgate_client
from vtdb import vtgate_cursor
from vtdb import dbexceptions

import utils
import environment
from protocols_flavor import protocols_flavor


pack_kid = struct.Struct('!Q').pack


def get_keyspace_id(row_id):
  return row_id


class TestMysqlctl(unittest.TestCase):

  def test_standalone(self):
    """Sample test for run_local_database.py as a standalone process."""

    # launch a backend database based on the provided topology and schema
    port = environment.reserve_ports(1)
    args = [environment.run_local_database,
            '--port', str(port),
            '--topology',
            'test_keyspace/-80:test_keyspace_0,'
            'test_keyspace/80-:test_keyspace_1',
            '--schema_dir', os.path.join(environment.vttop, 'test',
                                         'vttest_schema'),
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

    # to test vtcombo:
    # ./vttest_sample_test.py -v -d
    # go install && vtcombo -port 15010 -grpc_port 15011 -service_map grpc-vtgateservice -topology test_keyspace/-80:test_keyspace_0,test_keyspace/80-:test_keyspace_1 -mycnf_server_id 1 -mycnf_socket_file $VTDATAROOT/vttest*/vt_0000000001/mysql.sock -db-config-dba-uname vt_dba -db-config-dba-charset utf8 -db-config-app-uname vt_app -db-config-app-charset utf8 -alsologtostderr
    # vtctl -vtgate_protocol grpc VtGateExecuteShards -server localhost:15011 -keyspace test_keyspace -shards -80 -tablet_type master "select 1 from dual"
    # vtctl -vtgate_protocol grpc VtGateExecuteKeyspaceIds -server localhost:15011 -keyspace test_keyspace -keyspace_ids 20 -tablet_type master "show tables"
    utils.pause('good time to test vtcombo with database running')

    protocol = protocols_flavor().vttest_protocol()
    if protocol == 'grpc':
      vtagte_addr = 'localhost:%d' % config['grpc_port']
    else:
      vtagte_addr = 'localhost:%d' % config['port']
    conn_timeout = 30.0

    # Connect to vtgate.
    conn = vtgate_client.connect(protocol, vtagte_addr, conn_timeout)

    # Insert a row.
    row_id = 123
    keyspace_id = get_keyspace_id(row_id)
    cursor = conn.cursor(
        'test_keyspace', 'master', keyspace_ids=[pack_kid(keyspace_id)],
        writable=True)
    cursor.begin()
    insert = ('insert into test_table (id, msg, keyspace_id) values (%(id)s, '
              '%(msg)s, %(keyspace_id)s)')
    bind_variables = {
        'id': row_id,
        'msg': 'test %s' % row_id,
        'keyspace_id': keyspace_id,
        }
    cursor.execute(insert, bind_variables)
    cursor.commit()

    # Read the row back.
    cursor.execute(
        'select * from test_table where id=%(id)s', {'id': row_id})
    result = cursor.fetchall()
    self.assertEqual(result[0][1], 'test 123')

    # try to insert again, see if we get the rigth integrity error exception
    # (this is meant to test vtcombo properly returns exceptions, and to a
    # lesser extend that the python client converts it properly)
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
      cursor.execute(insert, bind_variables)
    cursor.commit()

    # Try to fetch a large number of rows
    # (more than one streaming result packet).
    stream_cursor = conn.cursor(
        'test_keyspace', 'master', keyspace_ids=[pack_kid(keyspace_id)],
        cursorclass=vtgate_cursor.StreamVTGateCursor)
    stream_cursor.execute('select * from test_table where id >= %(id_start)s',
                          {'id_start': id_start})
    self.assertEqual(rowcount, len(list(stream_cursor.fetchall())))
    stream_cursor.close()

    # Clean up.
    cursor.close()
    conn.close()

    # and we're done, clean-up process
    sp.stdin.write('\n')
    sp.wait()

if __name__ == '__main__':
  utils.main()
