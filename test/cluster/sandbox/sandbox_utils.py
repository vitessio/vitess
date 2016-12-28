"""Sandbox util functions."""


def fix_shard_name(shard_name):
  if shard_name.startswith('-'):
    return 'x%s' % shard_name[1:]
  if shard_name.endswith('-'):
    return '%sx' % shard_name[:-1]
  return shard_name


def create_dependency_graph(objs, reverse=False):
  graph = {}
  names = [o.name for o in objs]
  for obj in objs:
    if reverse:
      graph[obj.name] = (
          [a.name for a in objs if obj.name in a.dependencies], obj)
    else:
      graph[obj.name] = (list(set(obj.dependencies).intersection(names)), obj)
  return graph

