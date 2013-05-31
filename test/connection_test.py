#!/usr/bin/env python
# coding: utf-8

import hmac
import json
import logging
import os
import subprocess
import shutil
import sys
import time
import unittest

from net import gorpc
from net import bsonrpc
from vtdb import vt_occ2
from vtdb import dbexceptions

LOGFILE = "/tmp/vtocc.log"
QUERYLOGFILE = "/tmp/vtocc_queries.log"

# This is a VtOCCConnection that doesn't attempt to do authentication.
class BsonConnection(bsonrpc.BsonRpcClient):
  def __init__(self, addr, timeout, user=None, password=None):
    bsonrpc.BsonRpcClient.__init__(self, addr, timeout, user, password)
    # Force uri to use the auth
    self.uri = 'http://%s/_bson_rpc_/auth' % self.addr


class BaseTest(unittest.TestCase):
  vtroot = os.getenv("VTROOT")
  vtdataroot = os.getenv("VTDATAROOT") or "/vt"
  tabletuid = "9460"
  mysql_port = 9460
  vtocc_port = 9461
  mysqldir = os.path.join(vtdataroot, "vt_0000009460")
  mysql_socket = os.path.join(mysqldir, "mysql.sock")
  credentials = {"ala": ["ma kota", "miala kota"]}
  credentials_file = os.path.join(mysqldir, 'authcredentials.json')
  dbconfig_file = os.path.join(mysqldir, "dbconf.json")
  dbconfig = {
      'charset': 'utf8',
      'dbname': 'vt_test',
      'host': 'localhost',
      'unix_socket': mysql_socket,
      'uname': 'vt_dba',  # use vt_dba as some tests depend on 'drop'
      'keyspace' : 'test_keyspace',
      'shard' : '0',
      }

  @property
  def vtocc_uri(self):
    return "localhost:%s" % self.vtocc_port

  @classmethod
  def dump_config_files(klass):
    with open(klass.credentials_file, 'w') as f:
      json.dump(klass.credentials, f)

    with open(klass.dbconfig_file, 'w') as f:
      json.dump(klass.dbconfig, f)

  @classmethod
  def _setUpClass(klass):
    os.mkdir(klass.mysqldir)
    klass.dump_config_files()
    klass.init_mysql()
    klass.start_vtocc()

  @classmethod
  def start_vtocc(klass):

    klass.user = str(klass.credentials.keys()[0])
    klass.password = str(klass.credentials[klass.user][0])
    klass.secondary_password = str(klass.credentials[klass.user][1])

    klass.vtstderr = open("/tmp/vtocc_stderr.log", "a+")
    # TODO(szopa): authcredentials
    klass.process = subprocess.Popen([klass.vtroot +"/bin/vtocc",
                                     "-port", str(klass.vtocc_port),
                                     "-auth-credentials", klass.credentials_file,
                                     "-dbconfig", klass.dbconfig_file,
                                     "-logfile", LOGFILE,
                                     "-querylog", QUERYLOGFILE],
                                    stderr=klass.vtstderr)
    time.sleep(1)
    connection = vt_occ2.VtOCCConnection("localhost:%s" % klass.vtocc_port, klass.dbconfig['keyspace'], klass.dbconfig['shard'], timeout=10, user=klass.user, password=klass.password)
    connection.dial()
    cursor = connection.cursor()
    cursor.execute("create table if not exists connection_test (c int)")
    connection.begin()
    cursor.execute("delete from connection_test")
    cursor.execute("insert into connection_test values (1), (2), (3), (4)")
    connection.commit()

  @classmethod
  def _tearDownClass(klass):
    try:
      klass.kill_vtocc()
    except AttributeError:
      pass

    # stop mysql, delete directory
    result = subprocess.call([
        klass.vtroot+"/bin/mysqlctl",
        "-tablet-uid",  klass.tabletuid,
        "teardown", "-force"
        ])
    if result != 0:
      raise Exception("cannot stop mysql")
    try:
      shutil.rmtree(klass.mysqldir)
    except OSError:
      pass

  @classmethod
  def kill_vtocc(klass):
    klass.process.kill()
    klass.process.wait()

  @classmethod
  def init_mysql(klass):
    res = subprocess.call([
        os.path.join(klass.vtroot+"/bin/mysqlctl"),
        "-tablet-uid",  klass.tabletuid,
        "-port", str(klass.vtocc_port),
        "-mysql-port", str(klass.mysql_port),
        "init"
        ])
    if res != 0:
      raise Exception("cannot start mysql")

    klass.mysql_socket = os.path.join(klass.mysqldir, "mysql.sock")
    res = subprocess.call([
        "mysql",
        "-S",  klass.mysql_socket,
        "-u", "vt_dba",
        "-e", "create database vt_test ; set global read_only = off"])
    if res != 0:
      raise Exception("Cannot create vt_test database")


class TestAuthentication(BaseTest):

  def setUp(self):
    for i in range(30):
      try:
        self.conn = BsonConnection(self.vtocc_uri, 2)
        self.conn.dial()
      except dbexceptions.OperationalError:
        if i == 29:
          raise
        time.sleep(1)

  def call(self, *args, **kwargs):
    return self.conn.call(*args, **kwargs)

  def authenticate(self, user, password):
    self.conn.user = user
    self.conn.password = password
    self.conn.authenticate()

  def test_correct_credentials(self):
    self.authenticate(self.user, self.password)

  def test_secondary_credentials(self):
    self.authenticate(self.user, self.secondary_password)

  def test_incorrect_user(self):
    self.assertRaises(gorpc.AppError, self.authenticate, "romek", "ma raka")

  def test_incorrect_credentials(self):
    self.assertRaises(gorpc.AppError, self.authenticate, "ala", "nie ma kota")

  def test_challenge_is_used(self):
    challenge = ""
    proof =  "%s %s" %(self.user, hmac.HMAC(self.password, challenge).hexdigest())
    self.assertRaises(gorpc.AppError, self.call, 'AuthenticatorCRAMMD5.Authenticate', {"Proof": proof})

  def test_only_few_requests_are_allowed(self):
    for i in range(4):
      try:
        self.call('AuthenticatorCRAMMD5.GetNewChallenge', "")
      except gorpc.GoRpcError:
        break
    else:
      self.fail("Too many requests were allowed (%s)." % (i + 1))

  def test_authenticated_methods_are_available(self):
    self.authenticate(self.user, self.password)
    self.call('SqlQuery.GetSessionId', {
        "Keyspace": self.dbconfig['keyspace'],
        "Shard": self.dbconfig['shard'],
        })


class TestConnection(BaseTest):

  def setUp(self):
    self.connection = vt_occ2.VtOCCConnection(self.vtocc_uri, self.dbconfig['keyspace'], self.dbconfig['shard'], timeout=1, user=self.user, password=self.password)
    self.connection.dial()

  def test_reconnect(self):
    cursor = self.connection.cursor()
    cursor.execute("create table if not exists connection_test (c int)")
    cursor.execute("select 1 from connection_test")
    try:
      cursor.execute("select sleep(1) from connection_test")
    except dbexceptions.DatabaseError as e:
      if "deadline exceeded" not in str(e):
        raise
    else:
      self.fail("Expected timeout error not raised")
    cursor.execute("select 2 from connection_test")

  def test_vtocc_not_there(self):
    connection = vt_occ2.VtOCCConnection("localhost:7777", self.dbconfig['keyspace'], self.dbconfig['shard'], timeout=1, user=self.user, password=self.password)
    self.assertRaises(dbexceptions.OperationalError, connection.dial)

  def test_vtocc_has_gone_away(self):
    cursor = self.connection.cursor()
    BaseTest.kill_vtocc()
    try:
      self.assertRaises(dbexceptions.OperationalError, cursor.execute, "select 1 from dual")
    finally:
      BaseTest.start_vtocc()

if __name__=="__main__":
  logging.getLogger().setLevel(logging.ERROR)
  try:
    BaseTest._setUpClass()
    unittest.main(argv=["auth_test.py"])
  finally:
    print "Waiting for processes to terminate...",
    sys.stdout.flush()
    BaseTest._tearDownClass()
    print "OK"
