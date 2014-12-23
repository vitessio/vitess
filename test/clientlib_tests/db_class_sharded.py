"""DB Classes for tables in sharded keyspace.

This provides examples of sharded db class and its interaction with
lookup tables.
"""

import topo_schema
from vtdb import db_object


class VtUser(db_object.DBObjectRangeSharded):
  keyspace = topo_schema.KS_RANGE_SHARDED[0]
  table_name = "vt_user"
  columns_list = ["id", "username", "msg", "keyspace_id"]


class VtUsernameLookup(db_object.DBObjectUnsharded):
  keyspace = topo_schema.KS_LOOKUP[0]
  table_name = "vt_username_lookup"
  columns_list = ["user_id", "username"]


class VtSongUserLookup(db_object.DBObjectUnsharded):
  keyspace = topo_schema.KS_LOOKUP[0]
  table_name = "vt_song_user_lookup"
  columns_list = ["song_id", "user_id"]
