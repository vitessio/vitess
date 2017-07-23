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

"""This is a demo for V3 features.

The script will launch all the processes necessary to bring up
the demo. It will bring up an HTTP server on port 8000 by default,
which you can override. Once done, hitting <Enter> will terminate
all processes. Vitess will always be started on port 12345.
"""

import json
import optparse
import os
import subprocess
import thread

from CGIHTTPServer import CGIHTTPRequestHandler
from BaseHTTPServer import HTTPServer

from google.protobuf import text_format

from vtproto import vttest_pb2


def start_http_server(port):
  httpd = HTTPServer(('', port), CGIHTTPRequestHandler)
  thread.start_new_thread(httpd.serve_forever, ())


def start_vitess():
  """This is the main start function."""

  topology = vttest_pb2.VTTestTopology()
  keyspace = topology.keyspaces.add(name='user')
  keyspace.shards.add(name='-80')
  keyspace.shards.add(name='80-')
  keyspace = topology.keyspaces.add(name='lookup')
  keyspace.shards.add(name='0')

  vttop = os.environ['VTTOP']
  args = [os.path.join(vttop, 'py/vttest/run_local_database.py'),
          '--port', '12345',
          '--proto_topo', text_format.MessageToString(topology,
                                                      as_one_line=True),
          '--web_dir', os.path.join(vttop, 'web/vtctld'),
          '--schema_dir', os.path.join(vttop, 'examples/demo/schema')]
  sp = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)

  # This load will make us wait for vitess to come up.
  print json.loads(sp.stdout.readline())
  return sp


def stop_vitess(sp):
  sp.stdin.write('\n')
  sp.wait()


def main():
  parser = optparse.OptionParser()
  parser.add_option('-p', '--port', default=8000, help='http server port')
  (options, unused_args) = parser.parse_args()

  sp = start_vitess()
  try:
    start_http_server(options.port)
    raw_input('\n'
              'Demo is running at: http://localhost:%d/\n'
              '\n'
              'Press enter to exit.\n' % options.port)
  finally:
    stop_vitess(sp)


if __name__ == '__main__':
  main()
