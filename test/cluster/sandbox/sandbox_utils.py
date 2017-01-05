"""Sandbox util functions."""


def fix_shard_name(shard_name):
  """Kubernetes doesn't allow '-' in attributes so replace it with an x.

  Example: -80 becomes x80, 80- becomes 80x.

  Args:
    shard_name: A standard shard name (like -80) (string).
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

