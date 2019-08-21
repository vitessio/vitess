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

"""Simple utility values, methods, and classes."""

import logging
import re
import time

from vtdb import dbexceptions
from vtdb import vtdb_logger
from vtproto import vtrpc_pb2

INITIAL_DELAY_MS = 5
NUM_RETRIES = 3
MAX_DELAY_MS = 100
BACKOFF_MULTIPLIER = 2


# This pattern is used in transient error messages to differentiate
# between a transient error and a throttling error.
throttler_err_re = re.compile(
    r'exceeded (.*) quota, rate limiting', re.IGNORECASE)


def log_exception(exc, keyspace=None, tablet_type=None):
  """This method logs the exception.

  Args:
    exc: exception raised by calling code
    keyspace: keyspace for the exception
    tablet_type: tablet_type for the exception
  """
  logger_object = vtdb_logger.get_logger()

  shard_name = None
  if isinstance(exc, dbexceptions.IntegrityError):
    logger_object.integrity_error(exc)
  else:
    logger_object.vtclient_exception(keyspace, shard_name, tablet_type, exc)


def exponential_backoff_retry(
    retry_exceptions,
    initial_delay_ms=INITIAL_DELAY_MS,
    num_retries=NUM_RETRIES,
    backoff_multiplier=BACKOFF_MULTIPLIER,
    max_delay_ms=MAX_DELAY_MS):
  """Decorator for exponential backoff retry.

  Log and raise exception if unsuccessful.
  Do not retry while in a session.

  Args:
    retry_exceptions: tuple of exceptions to check.
    initial_delay_ms: initial delay between retries in ms.
    num_retries: number max number of retries.
    backoff_multiplier: multiplier for each retry e.g. 2 will double the
      retry delay.
    max_delay_ms: upper bound on retry delay.

  Returns:
    A decorator method that returns wrapped method.
  """
  def decorator(method):
    """Returns wrapper that calls method and retries on retry_exceptions."""
    def wrapper(self, *args, **kwargs):
      attempt = 0
      delay = initial_delay_ms

      while True:
        try:
          return method(self, *args, **kwargs)
        except retry_exceptions as e:
          attempt += 1
          if attempt > num_retries or self.session:
            # In this case it is hard to discern keyspace
            # and tablet_type from exception.
            log_exception(e)
            raise e
          logging.error(
              'retryable error: %s, retrying in %d ms, attempt %d of %d', e,
              delay, attempt, num_retries)
          time.sleep(delay/1000.0)
          delay *= backoff_multiplier
          delay = min(max_delay_ms, delay)
    return wrapper
  return decorator


class VitessError(Exception):
  """VitessError is raised by an RPC with a server-side application error.

  VitessErrors have an error code and message.

  The individual protocols are responsible for getting the code and message
  from their protocol-specific encoding, and creating this error.
  Then this error can be converted to the right dbexception.
  """

  _errno_pattern = re.compile(r'\(errno (\d+)\)')

  def __init__(self, method_name, code, message):
    """Initializes a VitessError from a code and message.

    Args:
      method_name: RPC method name, as a string, that was called.
      code: integer that represents the error code. From vtrpc_pb2.Code.
      message: string representation of the error.
    """
    self.method_name = method_name
    self.code = code
    self.message = message
    # Make self.args reflect the error components
    super(VitessError, self).__init__(message, method_name, code)

  def __str__(self):
    """Print the error nicely, converting the proto error enum to its name."""
    return '%s returned %s with message: %s' % (
        self.method_name, vtrpc_pb2.Code.Name(self.code), self.message)

  def convert_to_dbexception(self, args):
    """Converts from a VitessError to the appropriate dbexceptions class.

    Args:
      args: argument tuple to use to create the new exception.

    Returns:
      An exception from dbexceptions.
    """
    # FIXME(alainjobart): this is extremely confusing: self.message is only
    # used for integrity errors, and nothing else. The other cases
    # have to provide the message in the args.
    if self.code == vtrpc_pb2.UNAVAILABLE:
      if throttler_err_re.search(self.message):
        return dbexceptions.ThrottledError(args)
      return dbexceptions.TransientError(args)
    if self.code == vtrpc_pb2.FAILED_PRECONDITION:
      return dbexceptions.QueryNotServed(args)
    if self.code == vtrpc_pb2.ALREADY_EXISTS:
      # Prune the error message to truncate after the mysql errno, since
      # the error message may contain the query string with bind variables.
      msg = self.message.lower()
      parts = self._errno_pattern.split(msg)
      pruned_msg = msg[:msg.find(parts[2])]
      new_args = (pruned_msg,) + tuple(args[1:])
      return dbexceptions.IntegrityError(new_args)
    if self.code == vtrpc_pb2.INVALID_ARGUMENT:
      return dbexceptions.ProgrammingError(args)
    return dbexceptions.DatabaseError(args)


def unique_join(str_list, delim='|'):
  return delim.join(sorted(set(str(item) for item in str_list)))


def keyspace_id_prefix(packed_keyspace_id):
  """Return the first str byte of packed_keyspace_id if it exists."""
  return '%02x' % ord(packed_keyspace_id[0])


def keyspace_id_prefixes(packed_keyspace_ids):
  """Return the first str byte of each packed_keyspace_id if it exists."""
  return unique_join(keyspace_id_prefix(pkid) for pkid in packed_keyspace_ids)


def convert_exception_kwarg(key, value):
  if value is None:
    return key, value
  if key in (
      'entity_column_name',
      'keyspace',
      'num_queries',
      'sql',
      'tablet_type'):
    return key, value
  elif key == 'entity_keyspace_id_map':
    return 'entity_keyspace_ids', keyspace_id_prefixes(
        value.values())
  elif key in (
      'keyspace_ids',
      'merged_keyspace_ids'):
    return key, keyspace_id_prefixes(value)
  elif key in (
      'keyranges',
      'keyspaces',
      'sqls'):
    return key, unique_join(value)
  elif key in (
      'not_in_transaction',
      'as_transaction'):
    return key, str(value)
  else:
    return key, 'unknown'


def convert_exception_kwargs(kwargs):
  """Convert kwargs into a readable str.

  Args:
    kwargs: A (str: value) dict.

  Returns:
    A comma-delimited string of converted, truncated key=value pairs.
      All non-None kwargs are included in alphabetical order.
  """
  new_kwargs = {}
  for key, value in kwargs.iteritems():
    new_key, new_value = convert_exception_kwarg(key, value)
    new_kwargs[new_key] = new_value
  return ', '.join(
      ('%s=%s' % (k, v))[:256]
      for (k, v) in sorted(new_kwargs.iteritems())
      if v is not None)
