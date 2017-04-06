#!/usr/bin/env python
"""Create schema based on an input sql file."""

import logging
import optparse
import os
import vtctl_sandbox


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
