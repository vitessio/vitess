#!/usr/bin/env python

# Copyright 2015 Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""This sample test demonstrates how to setup and teardown a test
database with the associated Vitess processes. It is meant to be used
as a template for unit tests.

This unit test is written in python, but we don't depend on this at all.
We just execute py/vttest/local_database.py, as we would in any language.
"""

import json
import os
import subprocess
import unittest
import urllib

import utils
import environment

class TestMysqlctl(unittest.TestCase):
  def test_basic(self):

    # launch a backend database based on the provided topology and schema
    port = environment.reserve_ports(1)
    args = [os.path.join(environment.vtroot, 'py-vtdb', 'vttest',
                         'run_local_database.py'),
            '--port', str(port),
            '--topology',
            'test_keyspace/-80:test_keyspace_0,'
            'test_keyspace/80-:test_keyspace_1',
            '--schema_dir', os.path.join(environment.vttop, 'test',
                                         'vttest_schema')]
    sp = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    config = json.loads(sp.stdout.readline())

    # gather the vars for the vtgate process
    url = 'http://localhost:%d/debug/vars' % config['port']
    f = urllib.urlopen(url)
    data = f.read()
    f.close()
    vars = json.loads(data)
    self.assertIn('vtgate', vars['cmdline'][0])

    # and we're done, clean-up
    sp.stdin.write('\n')
    sp.wait()

if __name__ == '__main__':
  utils.main()
