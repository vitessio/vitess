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
