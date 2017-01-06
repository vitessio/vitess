"""Sandbox util functions."""

import datetime
import os
import random


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


def create_dependency_graph(objs, reverse=False):
  """Creates a dependency graph based on dependencies attributes.

  Args:
    objs: A list of objects where each one has an attribute named dependencies.
    reverse: Whether to reverse the dependencies (bool).

  Returns:
    A map of object names to a pair of a list of dependent object names and
    the object itself {string: ([string], obj)}
  """
  graph = {}
  names = [o.name for o in objs]
  for obj in objs:
    if reverse:
      dependencies = [a.name for a in objs if obj.name in a.dependencies]
    else:
      dependencies = list(set(obj.dependencies).intersection(names))
    graph[obj.name] = {
        'dependencies': dependencies,
        'object': obj,
    }
  return graph


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
  adjectives = []
  animals = []
  with open('naming/adjectives.txt', 'r') as f:
    adjectives = [a for a in f.read().split('\n') if a]
  with open('naming/animals.txt', 'r') as f:
    animals = [a for a in f.read().split('\n') if a]
  return '%s%s' % (random.choice(adjectives), random.choice(animals))

