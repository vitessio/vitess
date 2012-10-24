# coding: utf-8
import hmac
import json
import optparse
import os
import subprocess
import time
import unittest

from vtdb import vt_occ2 as db
from vtdb import dbexceptions
from vtdb import tablet2
from net import gorpc

LOGFILE = "/tmp/vtocc.log"
QUERYLOGFILE = "/tmp/vtocc_queries.log"

# This is a VtOCCConnection that doesn't attempt to do authentication.
class BareOCCConnection(db.VtOCCConnection):
  def dial(self):
    tablet2.TabletConnection.dial(self)

  @property
  def uri(self):
    return 'http://%s/_bson_rpc_/auth' % self.addr


class TestAuthentication(unittest.TestCase):
  vtroot = os.getenv("VTROOT")

  def setUp(self):
    for i in range(30):
      try:
        self.conn = BareOCCConnection("localhost:9461", None, 2)
        return
      except dbexceptions.OperationalError:
        if i == 29:
          raise
        time.sleep(1)

  @classmethod
  def setUpVTOCC(klass, dbconfig, authcredentials):

    with open(dbconfig) as f:
      klass.cfg = json.load(f)

    with open(authcredentials) as f:
      klass.credentials = json.load(f)

    klass.vtstderr = open("/tmp/vtocc_stderr.log", "a+")
    klass.process = subprocess.Popen([klass.vtroot +"/bin/vtocc",
                                     "-port", "9461",
                                     "-auth-credentials", authcredentials,
                                     "-dbconfig", dbconfig,
                                     "-logfile", LOGFILE,
                                     "-querylog", QUERYLOGFILE],
                                    stderr=klass.vtstderr)
    time.sleep(1)

  @classmethod
  def tearDownVTOCC(klass):
    try:
      klass.process.kill()
      klass.process.wait()
    except AttributeError:
      pass

  def call(self, *args, **kwargs):
    return self.conn.client.call(*args, **kwargs)

  def authenticate(self, user, password):
    challenge = self.call('AuthenticatorCRAMMD5.GetNewChallenge', "").reply['Challenge']
    proof = user + " " + hmac.HMAC(str(password), challenge).hexdigest()
    return self.call('AuthenticatorCRAMMD5.Authenticate', {"Proof": proof})

  def test_correct_credentials(self):
    self.authenticate("ala", "ma kota")

  def test_secondary_credentials(self):
    self.authenticate("ala", u"miala kota")

  def test_incorrect_user(self):
    self.assertRaises(gorpc.AppError, self.authenticate, "romek", "ma raka")

  def test_incorrect_credentials(self):
    self.assertRaises(gorpc.AppError, self.authenticate, "ala", "nie ma kota")

  def test_challenge_is_used(self):
    challenge = ""
    proof =  "ala " + hmac.HMAC("ma kota", challenge).hexdigest()
    self.assertRaises(gorpc.AppError, self.call, 'AuthenticatorCRAMMD5.Authenticate', {"Proof": proof})

  def test_only_few_requests_are_allowed(self):
    for i in range(4):
      try:
        self.call('AuthenticatorCRAMMD5.GetNewChallenge', "")
      except gorpc.GoRpcError as e:
        break
    else:
      self.fail("Too many requests were allowed (%s)." % (i + 1))

  def test_authenticated_methods_are_available(self):
    self.authenticate("ala", "ma kota")
    self.call('OccManager.GetSessionId', '<test db name>')


if __name__=="__main__":
  parser = optparse.OptionParser(usage="usage: %prog [options]")
  parser.add_option("-c", "--dbconfig", action="store", dest="dbconfig", default="dbtest.json",
                    help="json db config file")
  parser.add_option("-a", "--authentication-credentials", action="store", dest="auth_credentials", default="authcredentials_test.json",
                    help="json CRAM-MD5 credentials file")

  (options, args) = parser.parse_args()

  try:
    TestAuthentication.setUpVTOCC(options.dbconfig, options.auth_credentials)
    unittest.main(argv=["auth_test.py"])
  finally:
    print "Waiting for vtocc to terminate...",
    TestAuthentication.tearDownVTOCC()
    print "OK"
