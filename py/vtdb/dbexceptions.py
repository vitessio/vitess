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

import exceptions


class Error(exceptions.StandardError):
  pass


class DatabaseError(exceptions.StandardError):
  pass


class DataError(DatabaseError):
  pass


class Warning(exceptions.StandardError):
  pass


class InterfaceError(Error):
  pass


class InternalError(DatabaseError):
  pass


class OperationalError(DatabaseError):
  pass


class ProgrammingError(DatabaseError):
  pass


class NotSupportedError(ProgrammingError):
  pass


class IntegrityError(DatabaseError):
  pass


class PartialCommitError(IntegrityError):
  pass


# Below errors are VT specific


# Retry means a simple and immediate reconnect to the same host/port
# will likely fix things. This is initiated by a graceful restart on
# the server side. In general this can be handled transparently
# unless the error is within a transaction.
class RetryError(OperationalError):
  pass


# This failure is "permanent" - retrying on this host is futile. Push the error
# up in case the upper layers can gracefully recover by reresolving a suitable
# endpoint.
class FatalError(OperationalError):
  pass


# This failure is operational in the sense that we must teardown the
# connection to ensure future RPCs are handled correctly.
class TimeoutError(OperationalError):
  pass


class TxPoolFull(DatabaseError):
  pass


# TransientError is raised for an error that is expected to go away soon. These
# errors should be retried. Examples: when a client exceedes allocated quota on
# a server, or when there's a backlog of requests and new ones are temporarily
# being rejected.
class TransientError(DatabaseError):
  pass


# TODO(aaijazi): These are deprecated. They will be replaced by TransientError.
# ThrottledError is raised when client exceeds allocated quota on the server
class ThrottledError(DatabaseError):
  pass


# QueryNotServed is raised when a pre-condition has failed. For instance,
# an update stream query cannot be served because there aren't enough
# binlogs on the server.
class QueryNotServed(DatabaseError):
  pass
