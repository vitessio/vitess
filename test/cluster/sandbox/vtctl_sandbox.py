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

"""Wrapper around vtctl execute_vtctl_command for sandboxes.

Note: This also provides a backup option of using kvtctl.sh, a kubernetes script
used to temporarily forward a port if vtctld has no forwarded port.

TODO(thompsonja): This is heavily tied to the kubernetes and will need to be
updated if other systems are used.
"""

import json
import os
import subprocess
import threading
import time


class Command(object):

  def __init__(self, cmd):
    self.cmd = cmd
    self.process = None
    self.stdout = None
    self.stderr = None

  def run(self, timeout_s):
    """Runs the vtctl command."""
    def target():
      self.process = subprocess.Popen(self.cmd, stdout=subprocess.PIPE)
      self.stdout, self.stderr = self.process.communicate()

    thread = threading.Thread(target=target)
    thread.start()

    thread.join(timeout_s)
    if thread.is_alive():
      self.process.terminate()
      thread.join()
    return self.process.returncode


def execute_vtctl_command(vtctl_args, namespace='default', timeout_s=180):
  """Executes a vtctl command with some retry logic."""
  vtctl_cmd_args = []
  vtctld_info = json.loads(subprocess.check_output(
      ['kubectl', 'get', 'service', 'vtctld', '--namespace=%s' % namespace,
       '-o', 'json']))
  try:
    # Check to see if the vtctld service has a forwarded port.
    ip = vtctld_info['status']['loadBalancer']['ingress'][0]['ip']
    vtctl_cmd_args = ['vtctlclient', '-server', '%s:15999' % ip] + vtctl_args
  except (KeyError, IndexError):
    pass
  if not vtctl_cmd_args:
    # Default to trying to use kvtctl.sh if a forwarded port cannot be found.
    os.environ['VITESS_NAME'] = namespace
    vtctl_cmd_args = (
        [os.path.join(os.environ['VTTOP'], 'examples/kubernetes/kvtctl.sh')]
        + vtctl_args)

  start_time = time.time()
  while time.time() - start_time < timeout_s:
    cmd = Command(vtctl_cmd_args)
    retcode = cmd.run(10)
    if cmd.stdout.startswith('Starting port forwarding'):
      # Ignore this extra output line if using kvtctl.sh
      cmd.stdout = cmd.stdout[cmd.stdout.find('\n')+1:]
    if retcode:
      last_error = 'Failed w/ errorcode %d, stdout %s, stderr %s' % (
          cmd.process.returncode, cmd.stdout, cmd.stderr)
    else:
      return cmd.stdout, True

  return ('Last error running %s: %s' % (' '.join(vtctl_cmd_args), last_error),
          False)
