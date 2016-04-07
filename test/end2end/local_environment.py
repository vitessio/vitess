"""A local test environment."""

import base_environment


class LocalEnvironment(base_environment.BaseEnvironment):
  """Environment for locally run instances, CURRENTLY UNSUPPORTED."""

  def __init__(self):
    super(LocalEnvironment, self).__init__()

  def create(self, **kwargs):
    pass
