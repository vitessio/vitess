#!/usr/bin/env python

import json
import logging
import optparse

import MySQLdb

def get_configuration(params, start, end):
  config = {'destination': params, 'tables': {}, 'keyrange': get_range(start, end)}
  conn = MySQLdb.connect(**params)
  cursor = conn.cursor()
  tables = config['tables']
  cursor.execute("SELECT table_name, avg_row_length FROM information_schema.tables WHERE table_schema = %s", params['db'])
  for name, row_length in cursor.fetchall():
    tables[name] = {'avg_row_length': row_length, 'columns': [], 'pk': []}

  cursor.execute("SELECT table_name, column_name FROM information_schema.columns where table_schema = %s order by table_name, ordinal_position", params['db'])
  for table, column in cursor.fetchall():
    tables[table]['columns'].append(column)

  cursor.execute("select table_name, column_name from information_schema.key_column_usage where table_schema=%s and constraint_name='PRIMARY' order by table_name, ordinal_position", params['db'])
  for table, column in cursor.fetchall():
    tables[table]['pk'].append(column)

  return config

def get_range(start, end):
  ret = {}
  if start != "":
    ret['start'] = int(start, 16)
  if end != "":
    ret['end'] = int(end, 16)
  return ret

if __name__=="__main__":
  parser = optparse.OptionParser(usage="usage: %prog [connection_param_name connection_param value]...")
  parser.add_option('--start', type='string', dest='start', default='',
                    help="keyrange start (hexadecimal)")
  parser.add_option('--end', type='string', dest='end', default='',
                    help="keyrange end (hexadecimal)")
  options, args = parser.parse_args()
  if len(args) %2 != 0:
    raise Exception("even number of arguments")

  params = {}
  key = None
  for i, arg in enumerate(args):
    if i % 2 == 0:
      key = arg
    else:
      params[key] = arg
  print json.dumps(get_configuration(params, options.start, options.end), indent=2)
