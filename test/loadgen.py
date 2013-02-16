#!/usr/bin/env python

import json
import logging
import optparse
import os
import subprocess
import sys

import utils

from queryservice_tests import test_env

create_table = """
create table vt_load(
  id bigint(20),
  num1 int(10) unsigned,
  char1 varchar(20),
  char2 varchar(20),
  num2 int(10) unsigned,
  char3 varchar(140),
  char4 char(1),
  char5 varchar(40),
  num3 int(10) unsigned,
  char6 varchar(300),
  char7 varchar(150),
  date1 date,
  char8 varchar(16),
  char9 varchar(120),
  num4 bigint(20),
  char10 char(1),
  num5 bigint(20),
  num6 bigint(20),
  num7 bigint(20),
  num8 int(11),
  char11 char(1),
  num9 tinyint(3) unsigned,
  num10 int(10) unsigned,
  num11 bigint(20),
  num12 bigint(20),
  num13 bigint(20),
  num14 int(10) unsigned,
  char12 varchar(10),
  num15 bigint(20) unsigned,
  num16 bigint(20) unsigned,
  num17 bigint(20) unsigned,
  num18 bigint(20),
  num19 int(10) unsigned,
  PRIMARY KEY (id)
) ENGINE=InnoDB
"""

insert = """
insert into vt_load values(
%(id)s,
%(num1)s,
%(char1)s,
%(char2)s,
%(num2)s,
%(char3)s,
%(char4)s,
%(char5)s,
%(num3)s,
%(char6)s,
%(char7)s,
%(date1)s,
%(char8)s,
%(char9)s,
%(num4)s,
%(char10)s,
%(num5)s,
%(num6)s,
%(num7)s,
%(num8)s,
%(char11)s,
%(num9)s,
%(num10)s,
%(num11)s,
%(num12)s,
%(num13)s,
%(num14)s,
%(char12)s,
%(num15)s,
%(num16)s,
%(num17)s,
%(num18)s,
%(num19)s
)
"""

fixed_values = {
    "num1": 1114367205,
    "char1": "abcdef",
    "char2": None,
    "num2": 8736,
    "char3": "asdasdas@asdasd.asdas",
    "char4": "a",
    "char5": "11.22.33.44",
    "num3": 0,
    "char6": "Asdihdfiuevkdj",
    "char7": "Basdfihjdsfoieuw",
    "date1": "1995-02-06",
    "char8": 98765,
    "char9": "Poidsf",
    "num4": 12323,
    "char10": "b",
    "num5": 4641528869078863271,
    "num6": 123,
    "num7": 12345,
    "num8": 784233,
    "char11": "A",
    "num9": 94,
    "num10": 1360128451,
    "num11": -1328510013,
    "num12": None,
    "num13": 89343,
    "num14": 384734,
    "char12": "en_US",
    "num15": 0,
    "num16": None,
    "num17": 12609028137540273996,
    "num18": 329527359100,
    "num19": 0,
}

def init_data(env):
  env.execute(create_table)
  env.execute("begin")
  for pk in range(10000):
    fixed_values["id"] = pk + 1
    env.execute(insert, fixed_values)
  env.execute("commit")

if __name__ == "__main__":
  parser = optparse.OptionParser(usage="usage: %prog [options] [test_names]")
  # Options used by test_env
  parser.add_option("-m", "--memcache", action="store_true", default=False,
                    help="starts a memcached, and tests rowcache")
  parser.add_option("-e", "--env", default='vttablet,vtocc',
                    help="Environment that will be used. Valid options: vttablet, vtocc")
  parser.add_option("-q", "--quiet", action="store_const", const=0, dest="verbose", default=1)
  parser.add_option("-v", "--verbose", action="store_const", const=2, dest="verbose", default=1)
  parser.add_option("--no-build", action="store_true")

  # Options for the load generator
  parser.add_option("--gomaxprocs", type="int", default="8")
  parser.add_option("--goroutines", type="int", default=50)
  parser.add_option("--connections", type="int", default=15000)
  parser.add_option("--queries", type="int", default=3000000)

  (options, args) = parser.parse_args()
  utils.options = options
  logging.getLogger().setLevel(logging.ERROR)

  if options.env == 'vttablet':
    env = test_env.VttabletTestEnv()
  elif options.env == 'vtocc':
    env = test_env.VtoccTestEnv()
  else:
    raise Exception("Valid options for -e: vtocc, vttablet")

  try:
    os.putenv("GOMAXPROCS", "%d" % options.gomaxprocs)
    env.setUp()
    init_data(env)
    subprocess.call([
      'go',
      'run',
      '%s/test/goloadgen/goloadgen.go' % env.vttop,
      "--goroutines=%d" % options.goroutines,
      "--connections=%d" % options.connections,
      "--queries=%d" % options.queries])
    dvars = env.debug_vars()
    print dvars["memstats"]["PauseNs"]
    print dvars["memstats"]["BySize"]
    dvars["memstats"]["PauseNs"] = None
    dvars["memstats"]["BySize"] = None
    json.dump(dvars, sys.stdout, indent=2, sort_keys=True)
    print
    json.dump(env.query_stats(), sys.stdout, indent=2)
    print
  finally:
    env.tearDown()
