#!/usr/bin/env python
"""Waits for mysql to be in a good state."""

import logging
import optparse
import re
import sys
import time
import vtctl_sandbox


def get_all_tablets(cells, namespace):
  """Returns a list of all tablet names."""
  tablets = []
  cells = cells.split(',')
  for cell in cells:
    cell_tablets = vtctl_sandbox.execute_vtctl_command(
        ['ListAllTablets', cell], namespace=namespace)[0].split('\n')
    for t in cell_tablets:
      tablets.append(t.split(' ')[0])
  r = re.compile('.*-.*')
  tablets = filter(r.match, tablets)
  logging.info('Tablets: %s.', ', '.join(tablets))
  return tablets


def main():
  parser = optparse.OptionParser(usage='usage: %prog [options] [test_names]')
  parser.add_option('-n', '--namespace', help='Kubernetes namespace',
                    default='vitess')
  parser.add_option('-c', '--cells', help='Comma separated list of cells')
  parser.add_option('-t', '--tablet_count',
                    help='Total number of expected tablets', type=int)
  parser.add_option('-w', '--wait', help='Max wait time (s)', type=int,
                    default=300)
  logging.getLogger().setLevel(logging.INFO)

  options, _ = parser.parse_args()

  logging.info('Waiting for mysql to become healthy.')

  start_time = time.time()
  good_tablets = []
  tablets = []

  # Do this in a loop as the output of ListAllTablets may not be parseable
  # until all tablets have been started.
  while (time.time() - start_time < options.wait and
         len(tablets) < options.tablet_count):
    tablets = get_all_tablets(options.cells, options.namespace)
    logging.info('Expecting %d tablets, found %d tablets',
                 options.tablet_count, len(tablets))

  start_time = time.time()
  while time.time() - start_time < options.wait:
    for tablet in [t for t in tablets if t not in good_tablets]:
      _, success = vtctl_sandbox.execute_vtctl_command(
          ['ExecuteFetchAsDba', tablet, 'show databases'],
          namespace=options.namespace, timeout_s=1)
      if success:
        good_tablets.append(tablet)
    logging.info('%d of %d tablets healthy.', len(good_tablets), len(tablets))
    if len(good_tablets) == len(tablets):
      logging.info('All tablets healthy in %f seconds.',
                   time.time() - start_time)
      break
  else:
    logging.warn('Timed out waiting for tablets to be ready.')
    sys.exit(1)


if __name__ == '__main__':
  main()
