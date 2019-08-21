#!/usr/bin/env python

# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A keytar webdriver test."""

import json
import logging
import signal
import subprocess
import time
import os
from selenium import webdriver
import unittest
import urllib.request, urllib.error, urllib.parse

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
    req = urllib.request.Request('%s/test_request?password=foo' % self.flask_addr)
    req.add_header('Content-Type', 'application/json')
    urllib.request.urlopen(
        req, json.dumps({'repository': {'repo_name': 'test/image'}}))
    self._wait_for_complete_status()
    logging.info('Dummy test complete.')
    self.driver.find_element_by_partial_link_text('PASSED').click()
    self.assertIn('Dummy output.',
                  self.driver.find_element_by_tag_name('body').text)


if __name__ == '__main__':
  unittest.main()
