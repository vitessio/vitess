# zkns - naming service.
#
# This uses zookeeper to resolve a list of servers to answer a
# particular query type.
#
# Additionally, config information can be embedded nearby for future updates
# to the python client.

import collections
import json
import random

from zk import zkjson


SrvEntry = collections.namedtuple('SrvEntry',
                                  ('host', 'port', 'priority', 'weight'))

class ZknsError(Exception):
  pass


class ZknsAddr(zkjson.ZkJsonObject):
  # NOTE: attributes match Go implementation, hence capitalization
  _serializable_attributes = ('uid', 'host', 'port', 'named_port_map')


class ZknsAddrs(zkjson.ZkJsonObject):
  # NOTE: attributes match Go implementation, hence capitalization
  _serializable_attributes = ('entries',)
  def __init__(self):
    self.entries = []


def _sorted_by_srv_priority(entries):
  # Priority is ascending, weight is descending.
  entries.sort(key=lambda x: (x.priority, -x.weight))

  priority_map = collections.defaultdict(list)
  for entry in entries:
    priority_map[entry.priority].append(entry)

  shuffled_entries = []
  for priority, priority_entries in sorted(priority_map.iteritems()):
    if len(priority_entries) <= 1:
      shuffled_entries.extend(priority_entries)
      continue

    weight_sum = sum([e.weight for e in priority_entries])
    # entries with a priority are sorted in descreasing weight
    while weight_sum > 0 and priority_entries:
      s = 0
      n = random.randint(0, weight_sum)
      for entry in priority_entries:
        s += entry.weight
        if s >= n:
          shuffled_entries.append(entry)
          priority_entries.remove(entry)
          break

      weight_sum -= shuffled_entries[-1].weight

  return shuffled_entries

def _get_addrs(zconn, zk_path):
  data = zconn.get_data(zk_path)
  addrs = ZknsAddrs()
  json_addrs = json.loads(data)
  for entry in json_addrs['entries']:
    addr = ZknsAddr()
    addr.__dict__.update(entry)
    addrs.entries.append(addr)
  return addrs

# zkns_name: /zk/cell/vt/ns/path:_port - port is optional
def lookup_name(zconn, zkns_name):
  if ':' in zkns_name:
    zk_path, port_name = zkns_name.split(':')
    if not port_name.startswith('_'):
      raise ZknsError('invalid port name', port_name)
  else:
    zk_path = zkns_name
    port_name = None

  try:
    addrs = _get_addrs(zconn, zk_path)
    srv_entries = []
    for addr in addrs.entries:
      if port_name:
        try:
          srv_entry = SrvEntry(addr.host, addr.named_port_map[port_name], 0, 0)
        except KeyError:
          raise ZknsError('no named port', zk_path, port_name)
      else:
        srv_entry = SrvEntry(addr.host, addr.port, 0, 0)
      srv_entries.append(srv_entry)

    shuffled_entries = _sorted_by_srv_priority(srv_entries)
    if not shuffled_entries:
      raise ZknsError('no addresses for zk path', zk_path, port_name)
    return shuffled_entries
  except Exception as e:
    raise ZknsError('no addresses for zk path', zk_path, e)
