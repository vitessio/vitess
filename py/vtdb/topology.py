"""Deprecated module that holds keyspace / sharding methods."""

# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# DEPRECATED module, just one hardcoded function left, so vtrouting.py
# is not changed yet. Will be cleaned up soon.

from vtdb import keyrange_constants


def get_sharding_col(keyspace_name):
  _ = keyspace_name
  return 'keyspace_id', keyrange_constants.KIT_UINT64
