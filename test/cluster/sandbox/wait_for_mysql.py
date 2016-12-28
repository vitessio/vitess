#!/usr/bin/env python
"""Waits for mysql to be in a good state."""

import logging
import optparse
import time
import vtctl_sandbox


def main():
  parser = optparse.OptionParser(usage='usage: %prog [options] [test_names]')
  parser.add_option('-n', '--namespace', help='k8s namespace',
                    default='vitess')
  parser.add_option('-c', '--cells', help='Colon delimited list of cells',
                    default='test')
  parser.add_option('-k', '--keyspace', help='Keyspace name',
                    default='test_keyspace')
  parser.add_option('-s', '--shard_count', help='Number of shards', default=2)
  parser.add_option('-t', '--tablet_count', help='Number of tablets', default=3)
  parser.add_option('-u', '--starting_uid', help='Starting tablet uid',
                    default=0)
  logging.getLogger().setLevel(logging.INFO)

  options, _ = parser.parse_args()

  logging.info('Waiting for mysql to become healthy')

  cells = options.cells.split(':')

  tablets = []
  starting_cell_index = options.starting_uid
  if len(cells) > 1:
    starting_cell_index += 100000000
  for cell_index, cell in enumerate(cells):
    for shard_index in range(int(options.shard_count)):
      for tablet_index in range(int(options.tablet_count)):
        uid = (100 + shard_index * 100) + (
            tablet_index + starting_cell_index + cell_index * 100000000)
        tablets.append('%s-%010d' % (cell, uid))

  start_time = time.time()
  while time.time() - start_time < 300:
    good_tablets = []
    for tablet in tablets:
      _, success = vtctl_sandbox.execute_vtctl_command(
          ['ExecuteFetchAsDba', tablet, 'show databases'],
          namespace=options.namespace)
      if success:
        good_tablets.append(tablet)
    logging.info('%d of %d tablets good', len(good_tablets), len(tablets))
    if len(good_tablets) == len(tablets):
      logging.info('All tablets ready in %f seconds', time.time() - start_time)
      return


if __name__ == '__main__':
  main()
