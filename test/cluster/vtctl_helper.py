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

"""Helper module for running vtctl commands.

This module allows for retry logic to ensure that vtctl commands are properly
executed.  This should help reduce flakiness in the sandbox.
"""

import logging
import subprocess
import time

from vtctl import vtctl_client


class VtctlClientError(Exception):
  pass


class VtctlHelper(object):
  """Various functions for running vtctl commands."""

  def __init__(self, protocol, vtctl_addr):
    self.protocol = protocol
    self.client = None
    self.vtctl_addr = vtctl_addr
    if vtctl_addr and protocol != 'grpc':
      self.client = vtctl_client.connect(protocol, vtctl_addr, 30)

  def execute_vtctl_command(self, args, action_timeout=60.0, expect_fail=False,
                            max_wait_s=180.0):
    """Executes a vtctl command on a running vtctl job.

    This function attempts to execute on any running vtctl job, returning
    immediately when a call to execute_vtctl_command completes successfully.

    Args:
      args: args to pass to vtctl_client's execute_vtctl_command function
      action_timeout: total timeout for the action (float, in seconds)
      expect_fail: whether or not the vtctl command should fail (bool)
      max_wait_s: maximum amount of time to wait for success (float, in seconds)

    Returns:
      Result of executing vtctl command

    Raises:
      VtctlClientError: Could not successfully call execute_vtctl_command
    """
    start_time = time.time()
    while time.time() - start_time < max_wait_s:
      try:
        if self.protocol == 'grpc':
          results = subprocess.check_output(
              ['vtctlclient', '-vtctl_client_protocol', self.protocol,
               '-server', self.vtctl_addr] + args, stderr=subprocess.STDOUT)
        else:
          results = vtctl_client.execute_vtctl_command(
              self.client, args, action_timeout=action_timeout)
        return results
      except Exception as e:
        if expect_fail:
          logging.info('Expected vtctl error, got: %s', e.message or e.output)
          raise VtctlClientError('Caught an expected vtctl error')
        logging.info('Vtctl error (vtctl %s): %s',
                     ' '.join(args), e.message or e.output)
      time.sleep(5)
    raise VtctlClientError('Timed out on vtctl_client execute_vtctl_command')

  def execute_vtctl_command_until_success(
      self, args, max_wait_s=180.0, retry_wait_s=5.0):
    """Executes a vtctl command on a running vtctl job.

    This function attempts to execute on any running vtctl job, returning
    immediately when a call to execute_vtctl_command returns nothing.  Do not
    use this if you expect execute_vtctl_client to return data.

    Args:
      args: args to pass to vtctl_client's execute_vtctl_command function
      max_wait_s: maximum amount of time to wait for success (float, in seconds)
      retry_wait_s: time between vtctl calls to wait (float, in seconds)

    Raises:
      VtctlClientError: execute_vtctl_command never returned empty data
    """
    start_time = time.time()
    while time.time() - start_time < max_wait_s:
      try:
        if not self.execute_vtctl_command(args):
          return
      except VtctlClientError:
        pass
      time.sleep(retry_wait_s)
    raise VtctlClientError(
        'Timed out on vtctl_client execute_vtctl_command_until_success')
