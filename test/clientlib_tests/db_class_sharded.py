"""DB Classes for tables in sharded keyspace.

This provides examples of sharded db class and its interaction with
lookup tables.
"""

from Crypto.Cipher import AES
import struct

import topo_schema
from vtdb import db_object

pack_kid = struct.Struct('!Q').pack
unpack_kid = struct.Struct('!Q').unpack
encryption_key = '\xff'*24


def create_keyspace_id(sharding_key):
  data = pack_kid(sharding_key)
  crypter = AES.new(encryption_key, AES.MODE_ECB)
  encrypted = crypter.encrypt(data)
  return unpack_kid(encrypted)[0]

class VtUser(db_object.DBObjectRangeSharded):
  keyspace = topo_schema.KS_RANGE_SHARDED[0]
  table_name = "vt_user"
  columns_list = ["id", "username", "msg", "keyspace_id"]
  sharding_key_column_name = "id"
  entity_id_lookup_map = {"username": VtUsernameLookup}


  @classmethod
  def sharding_key_to_keyspace_id(class_, sharding_key):
    keyspace_id = create_keyspace_id(sharding_key)
    logging.info("keyspace_id %s" % keyspace_id)
    return keyspace_id

  @classmethod
  def is_sharding_key_valid(class_, sharding_key):
    return True
