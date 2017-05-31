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

"""Keytar tests."""

import json
import os
import unittest

import keytar


class KeytarTest(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    cls.timestamp = '20160101_0000'
    if not os.path.isdir('/tmp/testlogs'):
      os.mkdir('/tmp/testlogs')
    with open(
        '/tmp/testlogs/%s_unittest.py.log' % cls.timestamp, 'w') as testlog:
      testlog.write('foo')

  def test_validate_request(self):
    keytar._validate_request('foo', {'password': 'foo'})
    keytar._validate_request(None, {'password': 'foo'})
    keytar._validate_request(None, {})
    with self.assertRaises(keytar.KeytarError):
      keytar._validate_request('foo', {'password': 'foo2'})
    with self.assertRaises(keytar.KeytarError):
      keytar._validate_request('foo', {})

  def test_get_download_github_repo_args(self):
    github_config = {'repo': 'youtube/vitess', 'repo_prefix': 'foo'}

    github_clone_args, repo_dir = (
        keytar._get_download_github_repo_args('/tmp', github_config))
    self.assertEquals(
        github_clone_args,
        ['git', 'clone', 'https://github.com/youtube/vitess', '/tmp/foo'])
    self.assertEquals('/tmp/foo', repo_dir)

    github_config = {
        'repo': 'youtube/vitess', 'repo_prefix': 'foo', 'branch': 'bar'}
    github_clone_args, repo_dir = (
        keytar._get_download_github_repo_args('/tmp', github_config))
    self.assertEquals(
        github_clone_args,
        ['git', 'clone', 'https://github.com/youtube/vitess', '/tmp/foo', '-b',
         'bar'])
    self.assertEquals('/tmp/foo', repo_dir)

  def test_logs(self):
    # Check GET test_results with no results.
    tester = keytar.app.test_client(self)
    log = tester.get('/test_log?log_name=%s_unittest.py' % self.timestamp)
    self.assertEqual(log.status_code, 200)
    self.assertEqual(log.data, 'foo')

  def test_results(self):
    # Check GET test_results with no results.
    tester = keytar.app.test_client(self)
    test_results = tester.get('/test_results')
    self.assertEqual(test_results.status_code, 200)
    self.assertEqual(json.loads(test_results.data), [])

    # Create a test_result, GET test_results should return an entry now.
    keytar.results[self.timestamp] = {
        'timestamp': self.timestamp,
        'status': 'Start',
        'tests': {},
    }
    test_results = tester.get('/test_results')
    self.assertEqual(test_results.status_code, 200)
    self.assertEqual(
        json.loads(test_results.data),
        [{'timestamp': self.timestamp, 'status': 'Start', 'tests': {}}])

    # Call POST update_results, GET test_results should return a changed entry.
    tester.post(
        '/update_results', data=json.dumps(dict(
            timestamp='20160101_0000', status='Complete')),
        follow_redirects=True, content_type='application/json')
    test_results = tester.get('/test_results')
    self.assertEqual(test_results.status_code, 200)
    self.assertEqual(
        json.loads(test_results.data),
        [{'timestamp': self.timestamp, 'status': 'Complete', 'tests': {}}])


if __name__ == '__main__':
  unittest.main()
