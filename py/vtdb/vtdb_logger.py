# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import logging


# VtdbLogger's methods are called whenever something worth noting happens.
# The default behavior of the class is to log using the logging module.
# Registering a new implementation allows the client code to report the
# conditions to any custom reporting mechanism.
#
# We use this in the following cases:
# - error reporting (an exception happened)
# - performance logging (calls to other services took that long)
class VtdbLogger(object):

  # topo_keyspace_fetch is called when we successfully get a SrvKeyspace object.
  def topo_keyspace_fetch(self, keyspace_name, topo_rtt):
    logging.info("Fetched keyspace %s from topo_client in %f secs", keyspace_name, topo_rtt)

  # topo_empty_keyspace_list is called when we get an empty list of
  # keyspaces from topo server.
  def topo_empty_keyspace_list(self):
    logging.warning('topo_empty_keyspace_list')

  # topo_bad_keyspace_data is called if we generated an exception
  # when reading a keyspace. This is within an exception handler.
  def topo_bad_keyspace_data(self, keyspace_name):
    logging.exception('error getting or parsing keyspace data for %s',
                        keyspace_name)

  # topo_zkocc_error is called whenever we get a zkocc.ZkOccError
  # when trying to resolve an endpoint.
  def topo_zkocc_error(self, message, db_key, e):
    logging.warning('topo_zkocc_error: %s for %s: %s', message, db_key, e)

  # topo_exception is called whenever we get an exception when trying
  # to resolve an endpoint (that is not a zkocc.ZkOccError, these get
  # handled by topo_zkocc_error).
  def topo_exception(self, message, db_key, e):
    logging.warning('topo_exception: %s for %s: %s', message, db_key, e)


# registration mechanism for VtdbLogger
__vtdb_logger = VtdbLogger()


def register_vtdb_logger(logger):
  global __vtdb_logger
  __vtdb_logger = logger


def get_logger():
  return __vtdb_logger
