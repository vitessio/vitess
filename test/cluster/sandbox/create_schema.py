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

"""Create schema based on an input sql file."""

import logging
import optparse
import os
from . import vtctl_sandbox


def main():
  parser = optparse.OptionParser(usage='usage: %prog [options] [test_names]')
  parser.add_option(
      '-n', '--namespace', help='Kubernetes namespace', default='vitess')
  parser.add_option(
      '-k', '--keyspace', help='Keyspace name', default='test_keyspace')
  parser.add_option(
      '-d', '--drop_table', help='An optional table name to drop')
  parser.add_option(
      '-s', '--sql_file', help='File containing sql schema',
      default=os.path.join(
          os.environ['VTTOP'], 'examples/kubernetes/create_test_table.sql'))
  logging.getLogger().setLevel(logging.INFO)

  options, _ = parser.parse_args()
  with open(options.sql_file, 'r') as sql_file:
    sql = sql_file.read()
  if options.drop_table:
    vtctl_sandbox.execute_vtctl_command(
        ['ApplySchema', '-sql', 'drop table if exists %s' % options.drop_table,
         options.keyspace], namespace=options.namespace)
  vtctl_sandbox.execute_vtctl_command(
      ['ApplySchema', '-sql', sql, options.keyspace],
      namespace=options.namespace)


if __name__ == '__main__':
  main()
