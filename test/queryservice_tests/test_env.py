#!/usr/bin/env python

# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import contextlib
import json
import MySQLdb as mysql
import os
import shutil
import subprocess
import time
import urllib2
import uuid

from vtdb import tablet as tablet_conn
from vtdb import cursor
from vtdb import dbexceptions

import framework
import cases_framework
import environment
import tablet
import utils


class EnvironmentError(Exception):
  pass


class TestEnv(object):
  memcache = False
  port = 0
  querylog = None

  txlog_file = os.path.join(environment.vtlogroot, "txlog")

  tablet = tablet.Tablet(62344)
  vttop = environment.vttop
  vtroot = environment.vtroot

  def __init__(self, env):
    if env not in ['vttablet', 'vtocc']:
      raise EnvironmentError('unexptected env', env)
    self.env = env

  @property
  def port(self):
    return self.tablet.port

  # call postSetup after the derived class setUp is done.
  def postSetup(self):
    self.querylog = Querylog(self)

  # call preTeardown before the derived class tearDown does its cleanup.
  def preTeardown(self):
    if self.querylog:
      self.querylog.close()
    self.querylog = None

  @property
  def address(self):
    return "localhost:%s" % self.port

  def connect(self):
    c = tablet_conn.connect(self.address, '', 'test_keyspace', '0', 2, user='youtube-dev-dedicated', password='vtpass')
    c.max_attempts = 1
    return c

  def execute(self, query, binds=None, cursorclass=None):
    if binds is None:
      binds = {}
    curs = cursor.TabletCursor(self.conn)
    try:
      curs.execute(query, binds)
    except dbexceptions.OperationalError:
      self.conn = self.connect()
      raise
    return curs

  def url(self, path):
    return "http://localhost:%s/" % (self.port) + path

  def http_get(self, path, use_json=True):
    data = urllib2.urlopen(self.url(path)).read()
    if use_json:
      return json.loads(data)
    return data

  def debug_vars(self):
    return framework.MultiDict(self.http_get("/debug/vars"))

  def table_stats(self):
    return framework.MultiDict(self.http_get("/debug/table_stats"))

  def query_stats(self):
    return self.http_get("/debug/query_stats")

  def health(self):
    return self.http_get("/debug/health", use_json=False)

  def check_full_streamlog(self, fi):
    # FIXME(szopa): better test?
    for line in fi:
      if '"name":"bytes 4"' in line:
        print "FAIL: full streamlog doesn't contain all bind variables."
        return 1
    return 0

  def create_customrules(self, filename):
    with open(filename, "w") as f:
      f.write("""[{
        "Name": "r1",
        "Description": "disallow bindvar 'asdfg'",
        "BindVarConds":[{
          "Name": "asdfg",
          "OnAbsent": false,
          "Operator": "NOOP"
        }]
      }]""")

  def create_schema_override(self, filename):
    with open(filename, "w") as f:
      f.write("""[{
        "Name": "vtocc_view",
        "PKColumns": ["key2"],
        "Cache": {
          "Type": "RW",
          "Prefix": "view1"
        }
      }, {
        "Name": "vtocc_part1",
        "PKColumns": ["key2"],
        "Cache": {
          "Type": "W",
          "Table": "vtocc_view"
        }
      }, {
        "Name": "vtocc_part2",
        "PKColumns": ["key3"],
        "Cache": {
          "Type": "W",
          "Table": "vtocc_view"
        }
      }]""")

  def run_cases(self, cases):
    curs = cursor.TabletCursor(self.conn)
    error_count = 0

    for case in cases:
      if isinstance(case, basestring):
        curs.execute(case)
        continue
      try:
        failures = case.run(curs, self)
      except Exception:
        print "Exception in", case
        raise
      error_count += len(failures)
      for fail in failures:
        print "FAIL:", case, fail
    error_count += self.check_full_streamlog(open(self.querylog.path_full, 'r'))
    return error_count

  def setUp(self):
    utils.wait_procs([self.tablet.init_mysql()])
    self.tablet.mquery("", ["create database vt_test_keyspace", "set global read_only = off"])

    self.mysql_conn, mcu = self.tablet.connect('vt_test_keyspace')
    with open(os.path.join(self.vttop, "test", "test_data", "test_schema.sql")) as f:
      for line in f:
        line = line.rstrip()
        if line=='' or line.startswith("#"):
          continue
        mcu.execute(line, {})
    mcu.close()

    customrules = os.path.join(environment.tmproot, 'customrules.json')
    self.create_customrules(customrules)
    schema_override = os.path.join(environment.tmproot, 'schema_override.json')
    self.create_schema_override(schema_override)
    table_acl_config = os.path.join(environment.vttop, 'test', 'test_data', 'table_acl_config.json')

    if self.env == 'vttablet':
      environment.topo_server_setup()
      utils.run_vtctl('CreateKeyspace -force test_keyspace')
      self.tablet.init_tablet('master', 'test_keyspace', '0')
      self.tablet.start_vttablet(
              memcache=self.memcache,
              customrules=customrules,
              schema_override=schema_override,
              table_acl_config=table_acl_config,
              auth=True,
      )
    else:
      self.tablet.start_vtocc(
              memcache=self.memcache,
              customrules=customrules,
              schema_override=schema_override,
              table_acl_config=table_acl_config,
              auth=True,
              keyspace="test_keyspace", shard="0",
      )
    self.conn = self.connect()
    self.txlogger = utils.curl(self.url('/debug/txlog'), background=True, stdout=open(self.txlog_file, 'w'))
    self.txlog = framework.Tailer(open(self.txlog_file), flush=self.tablet.flush)
    self.log = framework.Tailer(open(os.path.join(environment.vtlogroot, '%s.INFO' % self.env)), flush=self.tablet.flush)
    self.postSetup()

  def tearDown(self):
    self.preTeardown()
    self.tablet.kill_vttablet()
    try:
      utils.wait_procs([self.tablet.teardown_mysql()])
    except:
      # FIXME: remove
      pass
    if getattr(self, "txlogger", None):
      self.txlogger.terminate()
    if self.env == 'vttablet':
      environment.topo_server_teardown()
    utils.kill_sub_processes()
    utils.remove_tmp_files()
    self.tablet.remove_tree()


class Querylog(object):

  def __init__(self, env):
    self.env = env
    self.id = str(uuid.uuid4())
    self.curl = utils.curl(self.env.url('/debug/querylog'), background=True, stdout=open(self.path, 'w'))
    self.curl_full = utils.curl(self.env.url('/debug/querylog?full=true'), background=True, stdout=open(self.path_full, 'w'))
    time.sleep(0.3)
    self.tailer = framework.Tailer(open(self.path), sleep=0.02)
    self.tailer_full = framework.Tailer(open(self.path_full), sleep=0.02)

  @property
  def path(self):
    return os.path.join(environment.vtlogroot, 'querylog' + self.id)

  @property
  def path_full(self):
    return os.path.join(environment.vtlogroot, 'querylog_full' + self.id)

  def reset(self):
    self.tailer.reset()
    self.tailer_full.reset()

  def close(self, *args, **kwargs):
    self.curl.terminate()
    self.curl_full.terminate()
