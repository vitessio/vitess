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

  #
  # vtclient callbacks
  #

  # Integrity Error is called when mysql throws an IntegrityError on a query.
  # This is thrown by both vtclient and vtgatev2.
  def integrity_error(self, e):
    logging.warning('integrity_error: %s', e)

  # vtclient_exception is called when a FatalError is raised by
  # vtclient (that error is sent back to the application, the retries
  # happen at a lower level). e can be one of
  # dbexceptions.{RetryError, FatalError, TxPoolFull}
  # or a more generic dbexceptions.OperationalError
  def vtclient_exception(self, keyspace_name, shard_name, db_type, e):
    logging.warning('vtclient_exception for %s.%s.%s: %s', keyspace_name,
                    shard_name, db_type, e)

  #
  # vtgatev2 callbacks
  #

  # vtgatev2_exception is called when we get an exception talking to vtgate.
  def vtgatev2_exception(self, e):
    logging.warning('vtgatev2_exception: %s', e)

  def log_private_data(self, private_data):
    logging.info('Additional exception data %s', private_data)

  def warning(self, msg, *args, **kwargs):
    logging.warning(msg, *args, **kwargs)


# registration mechanism for VtdbLogger
__vtdb_logger = VtdbLogger()


def register_vtdb_logger(logger):
  global __vtdb_logger
  __vtdb_logger = logger


def get_logger():
  return __vtdb_logger
