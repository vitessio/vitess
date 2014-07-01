import exceptions

class Error(exceptions.StandardError):
  pass

class DatabaseError(exceptions.StandardError):
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

class DeadlineExceededError(OperationalError):
  pass

# This failure is operational in the sense that we must teardown the connection to
# ensure future RPCs are handled correctly.
class TimeoutError(OperationalError):
  pass


class TxPoolFull(DatabaseError):
  pass
