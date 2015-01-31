"""Constants that describe different sharding schemes.

Different sharding schemes govern different routing strategies
that are computed while create the correct cursor.
"""

UNSHARDED = "UNSHARDED"
RANGE_SHARDED = "RANGE"
CUSTOM_SHARDED = "CUSTOM"

TABLET_TYPE_MASTER = 'master'
TABLET_TYPE_REPLICA = 'replica'
TABLET_TYPE_BATCH = 'rdonly'
