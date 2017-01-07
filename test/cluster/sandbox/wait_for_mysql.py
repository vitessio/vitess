#!/usr/bin/env python
"""Waits for mysql to be in a good state."""

import logging
import optparse
import time
import vtctl_sandbox


def main():
  parser = optparse.OptionParser(usage='usage: %prog [options] [test_names]')
  parser.add_option('-n', '--namespace', help='Kubernetes namespace',
                    default='vitess')
  parser.add_option('-c', '--cells', help='Comma separated list of cells')
  logging.getLogger().setLevel(logging.INFO)

  options, _ = parser.parse_args()

  logging.info('Waiting for mysql to become healthy.')

  tablets = []
  cells = options.cells.split(',')
  for cell in cells:
    cell_tablets = vtctl_sandbox.execute_vtctl_command(
        ['ListAllTablets', cell], namespace=options.namespace)[0].split('\n')
    for t in cell_tablets:
      tablets.append(t.split(' ')[0])
  tablets = filter(None, tablets)

  logging.info('Tablets: %s.', ', '.join(tablets))

  start_time = time.time()
  while time.time() - start_time < 300:
    good_tablets = []
    for tablet in [t for t in tablets if t not in good_tablets]:
      _, success = vtctl_sandbox.execute_vtctl_command(
          ['ExecuteFetchAsDba', tablet, 'show databases'],
          namespace=options.namespace)
      if success:
        good_tablets.append(tablet)
    logging.info('%d of %d tablets healthy.', len(good_tablets), len(tablets))
    if len(good_tablets) == len(tablets):
      logging.info('All tablets healthy in %f seconds.',
                   time.time() - start_time)
      break
  else:
    logging.warn('Timed out waiting for tablets to be ready.')


if __name__ == '__main__':
  main()
