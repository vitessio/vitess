"""Top component of a sandbox."""

import logging
import time

import sandbox_utils


class Sandlet(object):
  """Top-level component of a sandbox."""

  def __init__(self, name):
    self.name = name
    self.dependencies = []
    self.components = []

  def start(self):
    logging.info('Starting sandlet %s', self.name)
    component_graph = sandbox_utils.create_dependency_graph(self.components)
    while component_graph:
      components = [
          (k, v[1]) for (k, v) in component_graph.items() if not v[0]]
      for component_name, component in components:
        component.start()
        del component_graph[component_name]
        for _, (dependencies, _) in component_graph.items():
          if component_name in dependencies:
            dependencies.remove(component_name)
      ready_components = sum(x.is_up() for _, x in components)
      while ready_components < len(components):
        logging.info('Waiting for components to be ready: %d/%d',
                     ready_components, len(components))
        time.sleep(30)
        ready_components = sum(x.is_up() for _, x in components)

  def stop(self):
    component_graph = (
        sandbox_utils.create_dependency_graph(self.components, True))
    while component_graph:
      components = [
          (k, v[1]) for (k, v) in component_graph.items() if not v[0]]
      for component_name, component in components:
        component.stop()
        del component_graph[component_name]
        for _, (dependencies, _) in component_graph.items():
          if component_name in dependencies:
            dependencies.remove(component_name)
      down_components = sum(x.is_down() for _, x in components)
      while down_components < len(components):
        logging.info('Waiting for components to be down: %d/%d',
                     down_components, len(components))
        time.sleep(30)
        down_components = sum(x.is_down() for _, x in components)

