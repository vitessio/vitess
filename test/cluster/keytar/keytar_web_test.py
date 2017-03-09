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


def create_webdriver():
  """Creates a webdriver object (local or remote for Travis)."""
  if os.environ.get('CI') == 'true' and os.environ.get('TRAVIS') == 'true':
    username = os.environ['SAUCE_USERNAME']
    access_key = os.environ['SAUCE_ACCESS_KEY']
    capabilities = {}
    capabilities['tunnel-identifier'] = os.environ['TRAVIS_JOB_NUMBER']
    capabilities['build'] = os.environ['TRAVIS_BUILD_NUMBER']
    capabilities['platform'] = 'Linux'
    capabilities['browserName'] = 'chrome'
    hub_url = '%s:%s@localhost:4445' % (username, access_key)
    driver = webdriver.Remote(
        desired_capabilities=capabilities,
        command_executor='http://%s/wd/hub' % hub_url)
  else:
    os.environ['webdriver.chrome.driver'] = os.path.join(
        os.environ['VTROOT'], 'dist')
    # Only testing against Chrome for now
    driver = webdriver.Chrome()
    driver.set_window_position(0, 0)
    driver.set_window_size(1280, 1024)
  return driver


class TestKeytarWeb(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    cls.driver = create_webdriver()
    cls.flask_process = subprocess.Popen(
        ['./keytar.py', '--config_file=test_config.yaml',
         '--port=8080', '--password=foo'],
        preexec_fn=os.setsid)

  @classmethod
  def tearDownClass(cls):
    os.killpg(cls.flask_process.pid, signal.SIGTERM)
    cls.driver.quit()

  def _wait_for_complete_status(self, timeout_s=180):
    start_time = time.time()
    while time.time() - start_time < timeout_s:
      if 'Complete' in self.driver.find_element_by_id('results').text:
        break
      self.driver.refresh()
      time.sleep(5)
    else:
      self.fail('Timed out waiting for test to finish.')

  def test_keytar_web(self):
    self.driver.get('http://localhost:8080')
    req = urllib2.Request('http://localhost:8080/test_request?password=foo')
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
