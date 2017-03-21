#!/usr/bin/env python
"""A keytar webdriver test."""

import json
import logging
import signal
import subprocess
import time
import os
from selenium import webdriver
import unittest
import urllib2

import environment


class TestKeytarWeb(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    cls.driver = environment.create_webdriver()
    port = environment.reserve_ports(1)
    keytar_folder = os.path.join(environment.vttop, 'test/cluster/keytar')
    cls.flask_process = subprocess.Popen(
        [os.path.join(keytar_folder, 'keytar.py'),
         '--config_file=%s' % os.path.join(keytar_folder, 'test_config.yaml'),
         '--port=%d' % port, '--password=foo'],
        preexec_fn=os.setsid)
    cls.flask_addr = 'http://localhost:%d' % port

  @classmethod
  def tearDownClass(cls):
    os.killpg(cls.flask_process.pid, signal.SIGTERM)
    cls.driver.quit()

  def _wait_for_complete_status(self, timeout_s=180):
    start_time = time.time()
    while time.time() - start_time < timeout_s:
      if 'Complete' in self.driver.find_element_by_id('results').text:
        return
      self.driver.refresh()
      time.sleep(5)
    self.fail('Timed out waiting for test to finish.')

  def test_keytar_web(self):
    self.driver.get(self.flask_addr)
    req = urllib2.Request('%s/test_request?password=foo' % self.flask_addr)
    req.add_header('Content-Type', 'application/json')
    urllib2.urlopen(
        req, json.dumps({'repository': {'repo_name': 'test/image'}}))
    self._wait_for_complete_status()
    logging.info('Dummy test complete.')
    self.driver.find_element_by_partial_link_text('PASSED').click()
    self.assertIn('Dummy output.',
                  self.driver.find_element_by_tag_name('body').text)


if __name__ == '__main__':
  unittest.main()
