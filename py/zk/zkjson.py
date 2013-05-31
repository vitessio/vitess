# Implement a sensible wrapper that treats python objects as dictionaries
# with sensible restrictions on serialization.

import json

def _default(o):
  if hasattr(o, '_serializable_attributes'):
    return dict([(k, v)
                 for k, v in o.__dict__.iteritems()
                 if k in o._serializable_attributes])
  return o.__dict__

_default_kargs = {'default': _default,
                  'sort_keys': True,
                  'indent': 2,
                  }

def dump(*pargs, **kargs):
  _kargs = _default_kargs.copy()
  _kargs.update(kargs)
  return json.dump(*pargs, **_kargs)

def dumps(*pargs, **kargs):
  _kargs = _default_kargs.copy()
  _kargs.update(kargs)
  return json.dumps(*pargs, **_kargs)

load = json.load
loads = json.loads


class ZkJsonObject(object):
  _serializable_attributes = ()

  def to_json(self):
    return dumps(self)

  @classmethod
  def from_json(cls, data):
    o = cls()
    if data:
      o.__dict__.update(loads(data))
    return o
