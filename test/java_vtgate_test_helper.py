#!/usr/bin/env python

import vtgatev2_test
import utils
import sys
import json

def setUp():
  """Sets up VtGate for integration tests in Java client"""

  create_table = '''create table vtgate_test (
  id bigint auto_increment,
  name varchar(64),
  age SMALLINT,
  percent DECIMAL(5,2),
  keyspace_id bigint(20) unsigned NOT NULL,
  primary key (id)
  ) Engine=InnoDB'''
  vtgatev2_test.create_tables.append(create_table)
  utils.main(vtgatev2_test)
  # this is read in Java test to set up connection params
  sys.stdout.write(json.dumps({
      "port": vtgatev2_test.vtgate_port,
      "keyspace_name": vtgatev2_test.KEYSPACE_NAME,
      "shard_kid_map": vtgatev2_test.shard_kid_map,
  }))
  sys.stdout.flush()

if __name__ == '__main__':
  setUp()
