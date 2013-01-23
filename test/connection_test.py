#!/usr/bin/env python
# coding: utf-8

import hmac
import json
import os
import subprocess
import shutil
import time
import unittest

import MySQLdb
from net import gorpc
from vtdb import vt_occ2 as db
from vtdb import tablet2
from vtdb import dbexceptions

LOGFILE = "/tmp/vtocc.log"
QUERYLOGFILE = "/tmp/vtocc_queries.log"

# This is a VtOCCConnection that doesn't attempt to do authentication.
class BareOCCConnection(db.VtOCCConnection):
  @property
  def uri(self):
    return 'http://%s/_bson_rpc_/auth' % self.addr


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
    klass.start_mysql()
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
    connection = db.VtOCCConnection("localhost:%s" % klass.vtocc_port, klass.dbconfig['dbname'], timeout=10, user=klass.user, password=klass.password)
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
  def start_mysql(klass):
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
    self.conn = BareOCCConnection(self.vtocc_uri, None, 2)

  def call(self, *args, **kwargs):
    return self.conn.client.call(*args, **kwargs)

  def authenticate(self, user, password):
    challenge = self.call('AuthenticatorCRAMMD5.GetNewChallenge', "").reply['Challenge']
    proof = user + " " + hmac.HMAC(str(password), challenge).hexdigest()
    return self.call('AuthenticatorCRAMMD5.Authenticate', {"Proof": proof})

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
    self.call('OccManager.GetSessionId', self.dbconfig['dbname'])


class TestConnection(BaseTest):

  def setUp(self):
    self.connection = db.VtOCCConnection(self.vtocc_uri, self.dbconfig['dbname'], timeout=1, user=self.user, password=self.password)
    self.connection.dial()

  def test_reconnect(self):
    cursor = self.connection.cursor()
    cursor.execute("create table if not exists connection_test (c int)")
    cursor.execute("select 1 from connection_test")
    try:
      cursor.execute("select sleep(1) from connection_test")
    except MySQLdb.DatabaseError as e:
      if "deadline exceeded" not in e.args[1]:
        raise
    else:
      self.fail("Expected timeout error not raised")
    cursor.execute("select 2 from connection_test")

  def test_vtocc_not_there(self):
    connection = db.VtOCCConnection("localhost:7777", self.dbconfig['dbname'], timeout=1, user=self.user, password=self.password)
    self.assertRaises(dbexceptions.OperationalError, connection.dial)

  def test_vtocc_has_gone_away(self):
    cursor = self.connection.cursor()
    BaseTest.kill_vtocc()
    try:
      self.assertRaises(MySQLdb.OperationalError, cursor.execute, "select 1 from dual")
    finally:
      BaseTest.start_vtocc()

if __name__=="__main__":
  try:
    BaseTest._setUpClass()
    unittest.main(argv=["auth_test.py"])
  finally:
    print "Waiting for processes to terminate...",
    BaseTest._tearDownClass()
    print "OK"
