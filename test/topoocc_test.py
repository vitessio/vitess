#!/usr/bin/env python
import optparse
import sys
import unittest

import utils
from zk import zkocc


def setUpModule():
  try:
    utils.zk_setup()

  except:
    tearDownModule()
    raise


def tearDownModule():
  if utils.options.skip_teardown:
    return

  utils.zk_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()


class TopoOccTest(unittest.TestCase):
  def setUp(self):
    utils.zk_wipe()
    self.zkocc_server = utils.zkocc_start()
    self.topo = zkocc.TopoOccConnection("localhost:%u" % utils.zkocc_port_base, 30)
    self.topo.dial()

  def tearDown(self):
    utils.kill_sub_process(self.zkocc_server)

  def test_get_keyspaces(self):
    utils.run_vtctl('CreateKeyspace test_keyspace1')
    utils.run_vtctl('CreateKeyspace test_keyspace2')
    self.assertItemsEqual(self.topo.get_keyspaces(), ["test_keyspace1", "test_keyspace2"])


def main():
  parser = optparse.OptionParser(usage="usage: %prog [options] [test_names]")
  parser.add_option('--skip-teardown', action='store_true')
  parser.add_option('--teardown', action='store_true')
  parser.add_option("-q", "--quiet", action="store_const", const=0, dest="verbose", default=1)
  parser.add_option("-v", "--verbose", action="store_const", const=2, dest="verbose", default=1)
  parser.add_option("--no-build", action="store_true")

  (options, args) = parser.parse_args()

  utils.options = options
  global skip_teardown
  skip_teardown = options.skip_teardown
  if options.teardown:
    tearDownModule()
    sys.exit()
  unittest.main(argv=sys.argv[:1] + ['-f'])


if __name__ == '__main__':
  main()
