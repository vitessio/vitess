"""DB Classes for tables in sharded keyspace.

This provides examples of sharded db class and its interaction with
lookup tables.
"""

import logging
from Crypto.Cipher import DES3
import struct

import db_class_lookup
import topo_schema
from vtdb import db_object_range_sharded


pack_kid = struct.Struct('!Q').pack
unpack_kid = struct.Struct('!Q').unpack
encryption_key = '\xff'*24


def create_keyspace_id(sharding_key):
  data = pack_kid(sharding_key)
  crypter = DES3.new(encryption_key, DES3.MODE_ECB)
  encrypted = crypter.encrypt(data)
  return unpack_kid(encrypted)[0]


class VtRangeBase(db_object_range_sharded.DBObjectRangeSharded):
  @classmethod
  def sharding_key_to_keyspace_id(class_, sharding_key):
    keyspace_id = create_keyspace_id(sharding_key)
    return keyspace_id

  @classmethod
  def is_sharding_key_valid(class_, sharding_key):
    return True


class VtEntityRangeBase(db_object_range_sharded.DBObjectEntityRangeSharded):
  @classmethod
  def sharding_key_to_keyspace_id(class_, sharding_key):
    keyspace_id = create_keyspace_id(sharding_key)
    return keyspace_id

  @classmethod
  def is_sharding_key_valid(class_, sharding_key):
    return True


class VtUser(VtEntityRangeBase):
  keyspace = topo_schema.KS_RANGE_SHARDED[0]
  table_name = "vt_user"
  columns_list = ["id", "username", "msg", "keyspace_id"]
  sharding_key_column_name = "id"
  entity_id_lookup_map = {"username": db_class_lookup.VtUsernameLookup}
  column_lookup_name_map = {"id":"user_id"}


class VtSong(VtEntityRangeBase):
  keyspace = topo_schema.KS_RANGE_SHARDED[0]
  table_name = "vt_song"
  columns_list = ["id", "user_id", "title", "keyspace_id"]
  sharding_key_column_name = "user_id"
  entity_id_lookup_map = {"id": db_class_lookup.VtSongUserLookup}
  column_lookup_name_map = {"id":"song_id"}


class VtUserEmail(VtRangeBase):
  keyspace = topo_schema.KS_RANGE_SHARDED[0]
  table_name = "vt_user_email"
  columns_list = ["user_id", "email", "email_hash", "keyspace_id"]
  sharding_key_column_name = "user_id"
  entity_id_lookup_map = None


class VtSongDetail(VtRangeBase):
  keyspace = topo_schema.KS_RANGE_SHARDED[0]
  table_name = "vt_song_detail"
  columns_list = ["song_id", "album_name", "artist", "keyspace_id"]
  sharding_key_column_name = None
  entity_id_lookup_map = {"song_id": db_class_lookup.VtSongUserLookup}
