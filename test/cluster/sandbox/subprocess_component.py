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

"""Subprocess sandlet component."""

import logging
import subprocess

import sandbox
import sandbox_utils
import sandlet


class Subprocess(sandlet.SandletComponent):
  """A sandlet component for running scripts."""

  def __init__(self, name, sandbox_name, script, log_dir, **script_kwargs):
    super(Subprocess, self).__init__(name, sandbox_name)
    self.script = script
    self.script_kwargs = script_kwargs
    self.log_dir = log_dir

  def start(self):
    super(Subprocess, self).start()
    try:
      script_args = []
      for k, v in self.script_kwargs.items():
        script_args += ['--%s' % k, str(v)]
      logging.info('Executing subprocess script %s.', self.script)
      infofile = sandbox_utils.create_log_file(
          self.log_dir, '%s.INFO' % self.name)
      errorfile = sandbox_utils.create_log_file(
          self.log_dir, '%s.ERROR' % self.name)
      subprocess.check_call(
          ['./%s' % self.script] + script_args, stdout=infofile,
          stderr=errorfile)
      logging.info('Done.')
    except subprocess.CalledProcessError as error:
      raise sandbox.SandboxError(
          'Subprocess %s returned errorcode %d, find log at %s.' % (
              self.script, error.returncode, errorfile.name))
    finally:
      if infofile:
        infofile.close()
      if errorfile:
        errorfile.close()

  def stop(self):
    pass
