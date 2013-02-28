#!/usr/bin/env python
 # -*- coding: utf-8 -*-
import json
import logging
import optparse
import os
import sys
import tempfile
import unittest
import warnings

import tablet
import utils

from checkers import checker
from checkers import write_configuration


# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")


skip_teardown = False

# I need this mostly for mysql
destination_tablet = tablet.Tablet(62344, 6700, 3700)
source_tablets = [tablet.Tablet(62044, 6701, 3701),
                  tablet.Tablet(41983, 6702, 3702)]
tablets = [destination_tablet] + source_tablets

db_configuration = {
  "sources": [t.mysql_connection_parameters("test_checkers%i" % i) for i, t in enumerate(source_tablets)],
  "destination": destination_tablet.mysql_connection_parameters("test_checkers")
}

def setUpModule():
  utils.wait_procs([t.start_mysql() for t in tablets])

def tearDownModule():
  global skip_teardown
  if skip_teardown:
    return

  utils.wait_procs([t.teardown_mysql() for t in tablets], raise_on_error=False)
  utils.kill_sub_processes()
  for t in tablets:
    t.remove_tree()


class TestCheckers(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    config = dict(db_configuration)
    config["tables"] = {
    "test": {
      "columns": ["pk1", "pk2", "pk3", "msg"],
      "pk": ["pk1", "pk2", "pk3"],
      }
    }
    cls.configuration = config

  def setUp(self):
    create_table = "create table test (pk1 bigint, pk2 bigint, pk3 bigint, msg varchar(64), primary key (pk1, pk2, pk3)) Engine=InnoDB"
    destination_tablet.create_db("test_checkers")
    destination_tablet.mquery("test_checkers", create_table, True)
    for i, t in enumerate(source_tablets):
      t.create_db("test_checkers%s" % i)
      t.mquery("test_checkers%s" % i, create_table, True)

    destination_queries = []
    source_queries = [[] for t in source_tablets]
    for i in range(1, 400):
      query = "insert into test (pk1, pk2, pk3, msg) values (%s, %s, %s, 'message %s')" % (i/100+1, i/10+1, i, i)
      destination_queries.append(query)
      source_queries[i % 2].append(query)

    destination_tablet.mquery("test_checkers", destination_queries, write=True)
    for i, (tablet, queries) in enumerate(zip(source_tablets, source_queries)):
      tablet.mquery("test_checkers%s" % i, queries, write=True)
    directory = tempfile.mkdtemp()
    self.c = checker.Checker(self.configuration, 'test', directory, batch_count=20, logging_level=logging.WARNING)

  def tearDown(self):
    destination_tablet.mquery("test_checkers", "drop table test", True)
    for i, t in enumerate(source_tablets):
      t.mquery("test_checkers%s" % i, "drop table test", True)

  def query_all(self, sql, write=False):
    return [t.mquery("test_checkers", sql, write=write) for t in tablets]


  def test_ok(self):
    self.c._run()

  def test_different_value(self):
    destination_tablet.mquery("test_checkers", "update test set msg='something else' where pk2 = 29 and pk3 = 280 and pk1 = 3", write=True)
    with self.assertRaises(checker.Mismatch):
      self.c._run()

  def test_additional_value(self):
    destination_tablet.mquery("test_checkers", "insert into test (pk1, pk2, pk3) values (1, 1, 900)", write=True)
    with self.assertRaises(checker.Mismatch):
      self.c._run()

  def test_batch_size(self):
    self.configuration['tables']['test']['avg_row_length'] = 1024
    c = checker.Checker(self.configuration, 'test', tempfile.mkdtemp(), blocks=1, ratio=1.0)
    self.assertEqual(c.batch_size, 16)


class TestDifferentEncoding(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    config = dict(db_configuration)
    config["tables"] = {
    "test": {
      "columns": ["pk1", "pk2", "pk3", "msg"],
      "pk": ["pk1", "pk2", "pk3"],
      }
    }
    cls.configuration = config

  def setUp(self):
    create_table = "create table test (pk1 bigint, pk2 bigint, pk3 bigint, msg varchar(64), primary key (pk1, pk2, pk3)) Engine=InnoDB"
    destination_tablet.create_db("test_checkers")
    destination_tablet.mquery("test_checkers", create_table + "default character set = utf8", True)
    for i, t in enumerate(source_tablets):
      t.create_db("test_checkers%s" % i)
      t.mquery("test_checkers%s" % i, create_table + "default character set = latin2", True)

    destination_queries = []
    source_queries = [[] for t in source_tablets]
    source_connections = [t.connect('test_checkers%s' % i) for i, t in enumerate(source_tablets)]
    for c, _ in source_connections:
      c.set_character_set('latin2')
      c.begin()
    for i in range(1, 400):
      query = u"insert into test (pk1, pk2, pk3, msg) values (%s, %s, %s, '\xb1 %s')" % (i/100+1, i/10+1, i, i)
      destination_queries.append(query)
      #source_queries[i % 2].append(query.encode('utf-8').decode('iso-8859-2'))
      source_connections[i % 2][1].execute(query.encode('utf-8').decode('iso-8859-2'))
    for c, _ in source_connections:
      c.commit()

    destination_tablet.mquery("test_checkers", destination_queries, write=True)
    directory = tempfile.mkdtemp()
    self.c = checker.Checker(self.configuration, 'test', directory, batch_count=20, logging_level=logging.WARNING)

  def test_problem(self):
    with self.assertRaises(checker.Mismatch):
      self.c._run()


class TestConfguration(unittest.TestCase):
  def test_config(self):
    destination_tablet.create_db("test_checkers")
    destination_tablet.mquery(
      "test_checkers",
      "create table test1 (pk1 bigint, pk2 bigint, pk3 bigint, msg varchar(64), primary key (pk1, pk2, pk3)) Engine=InnoDB",
      True)
    destination_tablet.mquery(
      "test_checkers",
      "insert into test1 (pk1, pk2, pk3, msg) values (1, 1, 1, 'msg')",
      True)
    destination_tablet.mquery(
      "test_checkers",
      "create table test2 (pk1 bigint, pk2 bigint, msg varchar(64), primary key (pk1, pk2)) Engine=InnoDB",
      True)
    destination_tablet.mquery(
      "test_checkers",
      "insert into test2 (pk1, pk2, msg) values (1, 1, 'msg')",
      True)
    connection_params = destination_tablet.mysql_connection_parameters("test_checkers")
    config = write_configuration.get_configuration(connection_params)
    self.assertEqual(config['destination'], connection_params)
    self.assertItemsEqual(config['tables'].keys(), ['test1', 'test2'])
    self.assertEqual(config['tables']['test1']['columns'], ['pk1', 'pk2', 'pk3', 'msg'])
    self.assertEqual(config['tables']['test1']['pk'], ['pk1', 'pk2', 'pk3'])


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
  unittest.main(argv=sys.argv[:1])


if __name__ == '__main__':
  main()
