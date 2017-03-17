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
