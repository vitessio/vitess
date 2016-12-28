"""execute_vtctl_command."""

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

  def run(self, timeout):
    """Runs the vtctl command."""
    def target():
      self.process = subprocess.Popen(self.cmd, stdout=subprocess.PIPE)
      self.stdout, self.stderr = self.process.communicate()

    thread = threading.Thread(target=target)
    thread.start()

    thread.join(timeout)
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
    ip = vtctld_info['status']['loadBalancer']['ingress'][0]['ip']
    vtctl_cmd_args = ['vtctlclient', '-server', '%s:15999' % ip] + vtctl_args
  except Exception:  # pylint: disable=broad-except
    pass
  if not vtctl_cmd_args:
    os.environ['VITESS_NAME'] = namespace
    vtctl_cmd_args = (
        [os.path.join(os.environ['VTTOP'], 'examples/kubernetes/kvtctl.sh')]
        + vtctl_args)

  start_time = time.time()
  while time.time() - start_time < timeout_s:
    cmd = Command(vtctl_cmd_args)
    retcode = cmd.run(10)
    if cmd.stdout.startswith('Starting port forwarding'):
      cmd.stdout = cmd.stdout[cmd.stdout.find('\n')+1:]
    if retcode:
      last_error = 'Failed w/ errorcode %d, stdout %s, stderr %s' % (
          cmd.process.returncode, cmd.stdout, cmd.stderr)
    else:
      return cmd.stdout, True

  return ('Last error running %s: %s' % (' '.join(vtctl_cmd_args), last_error),
          False)
