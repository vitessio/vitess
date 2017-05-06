# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
    var = self.bind_vars[name]
    self.bind_vars[name]
    self.accessed_keys.add(name)
    if isinstance(var, (list, set, tuple)):
      return '::%s' % name

    return ':%s' % name

  def export_bind_vars(self):
    return dict([(k, self.bind_vars[k]) for k in self.accessed_keys])


# convert bind style from %(name)s to :name and export only the
# variables bound.
def prepare_query_bind_vars(query, bind_vars):
  bind_vars_proxy = BindVarsProxy(bind_vars)
  try:
    query %= bind_vars_proxy
  except KeyError as e:
    raise dbexceptions.InterfaceError(e[0], query, bind_vars)

  return query, bind_vars_proxy.export_bind_vars()
