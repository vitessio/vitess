from vtdb import dbexceptions

# A simple class to trap and re-export only variables referenced from
# the sql statement since bind dictionaries can be *very* noisy.  This
# is a by-product of converting the DB-API %(name)s syntax to our
# :name syntax.
class BindVarsProxy(object):
  def __init__(self, bind_vars):
    self.bind_vars = bind_vars
    self.accessed_keys = set()

  def __getitem__(self, name):
    self.bind_vars[name]
    self.accessed_keys.add(name)
    return ':%s' % name

  def export_bind_vars(self):
    return dict([(k, self.bind_vars[k]) for k in self.accessed_keys])


def prepare_query_bind_vars(query, bind_vars):
  bind_vars_proxy = BindVarsProxy(bind_vars)
  try:
    # convert bind style from %(name)s to :name
    query = query % bind_vars_proxy
  except KeyError as e:
    raise dbexceptions.InterfaceError(e[0], query, bind_vars)

  return query, bind_vars_proxy.export_bind_vars()
