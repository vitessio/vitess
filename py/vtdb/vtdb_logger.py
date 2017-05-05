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
"""This module defines the VtdbLogger interface, to report suspicious events.
"""

import logging


class VtdbLogger(object):
  """VtdbLogger's methods are called whenever something worth noting happens.

  The default behavior of the class is to log using the logging module.
  Registering a new implementation allows the client code to report the
  conditions to any custom reporting mechanism.

  We use this in the following cases:
  - error reporting (an exception happened)
  - performance logging (calls to other services took that long)
  """
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

  def log_private_data(self, private_data):
    logging.info('Additional exception data %s', private_data)

  def warning(self, msg, *args, **kwargs):
    logging.warning(msg, *args, **kwargs)


# registration mechanism for VtdbLogger
_vtdb_logger = VtdbLogger()


def register_vtdb_logger(logger):
  global _vtdb_logger
  _vtdb_logger = logger


def get_logger():
  return _vtdb_logger
