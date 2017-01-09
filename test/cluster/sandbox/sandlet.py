"""Top level component of a sandbox.

A sandlet is an abstraction to be used by applications to organize a sandbox
into discrete groupings. A user can start an entire sandbox, which will start
all sandlets, or selectively choose which ones to start.

Sandlets are made up of components, which are the actual subprocesses or jobs
to run.
"""

import logging

import sandbox_utils


class Sandlet(object):
  """Top-level component of a sandbox.

  Sandlets should be defined in a way to split applications in a logical way.
  """

  def __init__(self, name):
    self.name = name
    self.dependencies = []
    self.components = []

  def start(self):
    logging.info('Starting sandlet %s.', self.name)
    component_graph = sandbox_utils.create_dependency_graph(
        self.components, reverse=False)
    sandbox_utils.execute_dependency_graph(component_graph, True)

  def stop(self):
    logging.info('Stopping sandlet %s.', self.name)
    component_graph = sandbox_utils.create_dependency_graph(
        self.components, reverse=True)
    sandbox_utils.execute_dependency_graph(component_graph, False)

  def is_up(self):
    """Whether the component has finished being started."""
    return True

  def is_down(self):
    """Whether the component has finished being stopped."""
    return True


class SandletComponent(object):
  """Entity of a sandlet that encapsulates a process or job."""

  def __init__(self, name, sandbox_name):
    self.name = name
    self.sandbox_name = sandbox_name
    self.dependencies = []

  def start(self):
    logging.info('Starting component %s.', self.name)

  def stop(self):
    logging.info('Stopping component %s.', self.name)

  def is_up(self):
    """Whether the component has finished being started."""
    return True

  def is_down(self):
    """Whether the component has finished being stopped."""
    return True

