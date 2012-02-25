# Copyright 2012, Google Inc.
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:

#     * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
# copyright notice, this list of conditions and the following disclaimer
# in the documentation and/or other materials provided with the
# distribution.
#     * Neither the name of Google Inc. nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import errno
import logging
import re
import time

from net import gorpc
from vtdb import tablet2
from vtdb import dbexceptions

# NOTE(msolomon) this sketchy import allows upstream code to mostly interpret
# our exceptions as if they came from MySQLdb. Good for a cutover, probably
# bad long-term.
import MySQLdb as MySQLErrors

_errno_pattern = re.compile('\(errno (\d+)\)')

# NOTE(msolomon) This mapping helps us mimic the behavior of mysql errors
# even though the relationship between connections and failures is now quite
# different. In general, we map vtocc errors to DatabaseError, unless there
# is a pressing reason to be more precise. Otherwise, these errors can get
# misinterpreted futher up the call chain.

_mysql_error_map = {
  1062: MySQLErrors.IntegrityError,
}

# Errors fall into three classes based on recovery strategy.
#
# APP_LEVEL is for routine programmer errors (bad input etc) -- nothing can be
# done here, so just propagate the error upstream.
#
# RETRY means a simple reconnect (and immediate) reconnect to the same
# host will likely fix things. This is usually due vtocc restarting. In general
# this can be handled transparently unless the error is within a transaction.
#
# FATAL indicates that retrying an action on the host is likely to fail.

ERROR_APP_LEVEL = 'app_level'
ERROR_RETRY = 'retry'
ERROR_FATAL = 'fatal'

RECONNECT_DELAY = 0.002


# simple class to trap and re-export only variables referenced from the sql
# statement. bind dictionaries can be *very* noisy.
# this is by-product of converting the mysql %(name)s syntax to vtocc :name
class BindVarsProxy(object):
  def __init__(self, bind_vars):
    self.bind_vars = bind_vars
    self.accessed_keys = set()

  def __getitem__(self, name):
    self.bind_vars[name]
    self.accessed_keys.add(name)
    return ':%s' % name

  def export_bind_vars(self):
    return dict([(k, self.bind_vars[k]) for k in self.accessed_keys])


# Provide compatibility with the MySQLdb query param style and prune bind_vars
class VtOCCConnection(tablet2.TabletConnection):
  max_attempts = 2

  def dial(self):
    tablet2.TabletConnection.dial(self)
    try:
      response = self.client.call('OccManager.GetSessionId', self.dbname)
      self.set_session_id(response.reply)
    except gorpc.GoRpcError, e:
      raise dbexceptions.OperationalError(*e.args)

  def _convert_error(self, exception, *error_hints):
    message = str(exception[0]).lower()

    # NOTE(msolomon) extract a mysql error code so we can push this up the code
    # stack. At this point, this is almost exclusively for handling integrity
    # errors from duplicate key inserts.
    match = _errno_pattern.search(message)
    if match:
      err = int(match.group(1))
    elif isinstance(exception[0], IOError):
      err = exception[0].errno
    else:
      err = -1

    if message.startswith('fatal'):
      # Force this error code upstream so MySQL code understands this as a
      # permanent failure on this host. Feels a little dirty, but probably the
      # most consistent way since this correctly communicates the recovery
      # strategy upstream.
      raise MySQLErrors.OperationalError(2003, str(exception), self.addr,
                                         *error_hints)
    elif message.startswith('retry'):
      # Retry means that a trivial redial of this host will fix things. This
      # is frequently due to vtocc being restarted independently of the mysql
      # instance behind it.
      error_type = ERROR_RETRY
    elif 'curl error 7' in message:
      # Client side error - sometimes the listener is unavailable for a few
      # milliseconds during a restart.
      error_type = ERROR_RETRY
    elif err in (errno.ECONNREFUSED, errno.EPIPE):
      error_type = ERROR_RETRY
    else:
      # Everything else is app level - just process the failure and continue
      # to use the existing connection.
      error_type = ERROR_APP_LEVEL


    if error_type == ERROR_RETRY and self.transaction_id:
      # With a transaction, you cannot retry, so just redial. The next action
      # will be successful. Masquerade as commands-out-of-sync - an operational
      # error that can be reattempted at the app level.
      error_type = ERROR_APP_LEVEL
      error_hints += ('cannot retry action within a transaction',)
      try:
        time.sleep(RECONNECT_DELAY)
        self.dial()
      except Exception, e:
        # If this fails now, the code will retry later as the session_id
        # won't be valid until the handshake finishes.
        logging.warning('error dialing vtocc %s (%s)', self.addr, e)

    exc_class = _mysql_error_map.get(err, MySQLErrors.DatabaseError)
    return error_type, exc_class(err, str(exception), self.addr,
                                 *error_hints)

  def begin(self):
    attempt = 0
    while True:
      try:
        return tablet2.TabletConnection.begin(self)
      except dbexceptions.OperationalError, e:
        error_type, e = self._convert_error(e, 'begin')
        if error_type == ERROR_RETRY:
          attempt += 1
          if attempt < self.max_attempts:
            try:
              time.sleep(RECONNECT_DELAY)
              self.dial()
            except dbexceptions.OperationalError, dial_error:
              logging.warning('error dialing vtocc on begin %s (%s)',
                              self.addr, dial_error)
            continue
          logging.warning('Failing with 2003 on begin')
          raise MySQLErrors.OperationalError(2003, str(e), self.addr, 'begin')
        raise e

  def commit(self):
    try:
      return tablet2.TabletConnection.commit(self)
    except dbexceptions.OperationalError, e:
      error_type, e = self._convert_error(e, 'commit')
      raise e

  def _execute(self, sql, bind_variables):
    bind_vars_proxy = BindVarsProxy(bind_variables)
    try:
      # convert bind style from %(name)s to :name
      sql = sql % bind_vars_proxy
    except KeyError, e:
      raise dbexceptions.InterfaceError(e[0], sql, bind_variables)

    sane_bind_vars = bind_vars_proxy.export_bind_vars()

    attempt = 0
    while True:
      try:
        return tablet2.TabletConnection._execute(self, sql, sane_bind_vars)
      except dbexceptions.OperationalError, e:
        error_type, e = self._convert_error(e, sql, sane_bind_vars)
        if error_type == ERROR_RETRY:
          attempt += 1
          if attempt < self.max_attempts:
            try:
              time.sleep(RECONNECT_DELAY)
              self.dial()
            except dbexceptions.OperationalError, dial_error:
              logging.warning('error dialing vtocc on execute %s (%s)',
                              self.addr, dial_error)
            continue
          logging.warning('Failing with 2003 on %s: %s, %s', str(e), sql, sane_bind_vars)
          raise MySQLErrors.OperationalError(2003, str(e), self.addr, sql, sane_bind_vars)
        raise e

def connect(addr, timeout, dbname=None):
  conn = VtOCCConnection(addr, dbname, timeout)
  conn.dial()
  return conn
