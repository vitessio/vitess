"""Top level component of a sandbox.

A sandlet is an abstraction to be used by applications to organize a sandbox
into discrete groupings. A user can start an entire sandbox, which will start
all sandlets, or selectively choose which ones to start.

Sandlets are made up of components, which are the actual subprocesses or jobs
to run.
"""

import logging
import time

import sandbox_utils


class Sandlet(object):
  """Top-level component of a sandbox.

  Sandlets should be defined in a way to split applications in a logical way.
  """

  def __init__(self, name):
    self.name = name
    self.dependencies = []
    self.components = []

  def _execute_components(self, start):
    """Bring up or down the components."""
    component_graph = sandbox_utils.create_dependency_graph(
        self.components, reverse=not start)
    while component_graph:
      components = [x['object'] for x in component_graph.values()
                    if not x['dependencies']]
      for component in components:
        if start:
          component.start()
        else:
          component.stop()
        del component_graph[component.name]
        for _, v in component_graph.items():
          if component.name in v['dependencies']:
            v['dependencies'].remove(component.name)
      while True:
        if start:
          unfinished_components = [x.name for x in components if not x.is_up()]
        else:
          unfinished_components = [
              x.name for x in components if not x.is_down()]
        if not unfinished_components:
          break
        logging.info(
            'Waiting for components to be finished: %s.',
            ', '.join(unfinished_components))
        time.sleep(10)

  def start(self):
    logging.info('Starting sandlet %s.', self.name)
    self._execute_components(True)

  def stop(self):
    logging.info('Stopping sandlet %s.', self.name)
    self._execute_components(False)


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

