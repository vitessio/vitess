"""AWS environment."""

import base_environment


class AwsEnvironment(base_environment.BaseEnvironment):
  """Environment for AWS clusters.  CURRENTLY UNSUPPORTED."""

  def __init__(self):
    super(AwsEnvironment, self).__init__()

  def use_named(self, instance_name):
    pass

  def create(self, **kwargs):
    pass
