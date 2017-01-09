"""Sandbox util functions."""

import datetime
import logging
import os
import random
import time


def fix_shard_name(shard_name):
  """Kubernetes doesn't allow '-' in the beginning or end of attributes.

  Instead, replace them with an x.

  Example: -80 becomes x80, 80- becomes 80x.

  Args:
    shard_name: A standard shard name (like -80) (string).

  Returns:
    A fixed shard name suitable for kubernetes (string).
  """
  if shard_name.startswith('-'):
    return 'x%s' % shard_name[1:]
  if shard_name.endswith('-'):
    return '%sx' % shard_name[:-1]
  return shard_name


class DependencyError(Exception):
  pass


def create_dependency_graph(objs, reverse=False, subgraph=None):
  """Creates a dependency graph based on dependencies attributes.

  Args:
    objs: A list of objects where each one has an attribute named dependencies.
    reverse: Whether to reverse the dependencies (bool).
    subgraph: A list of object names to limit the graph to ([string]).

  Returns:
    A map of object names to a pair of a list of dependent object names and
    the object itself {string: ([string], obj)}
  """
  subgraph = subgraph or [x.name for x in objs]
  graph = {}
  for obj in objs:
    if obj.name not in subgraph:
      continue
    if reverse:
      dependencies = [a.name for a in objs
                      if obj.name in a.dependencies and a.name in subgraph]
    else:
      dependencies = list(set(obj.dependencies).intersection(subgraph))
    graph[obj.name] = {
        'dependencies': dependencies,
        'object': obj,
    }
  return graph


def execute_dependency_graph(graph, start):
  while graph:
    components = [x['object'] for x in graph.values()
                  if not x['dependencies']]
    if not components:
      # This is a cycle
      raise DependencyError(
          'Cycle detected: remaining dependency graph: %s.' % graph)
    for component in components:
      if start:
        component.start()
      else:
        component.stop()
      del graph[component.name]
      for _, v in graph.items():
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
          'Waiting to be finished: %s.', ', '.join(unfinished_components))
      time.sleep(10)


def create_log_file(log_dir, filename):
  """Create a log file.

  This function creates a timestamped log file, and updates a non-timestamped
  symlink in the log directory.

  Example: For a log called init.INFO, this function will create a log file
           called init.INFO.20170101-120000.100000 and update a symlink
           init.INFO to point to it.

  Args:
    log_dir: Base path for logs (string).
    filename: The base name of the log file (string).

  Returns:
    The opened file handle.
  """
  timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S.%f')
  symlink_name = os.path.join(log_dir, filename)
  timestamped_name = '%s.%s' % (symlink_name, timestamp)
  if os.path.islink(symlink_name):
    os.remove(symlink_name)
  os.symlink(timestamped_name, symlink_name)
  return open(timestamped_name, 'w')


def generate_random_name():
  with open('naming/adjectives.txt', 'r') as f:
    adjectives = [l.strip() for l in f if l.strip()]
  with open('naming/animals.txt', 'r') as f:
    animals = [l.strip() for l in f if l.strip()]
  return '%s%s' % (random.choice(adjectives), random.choice(animals))

