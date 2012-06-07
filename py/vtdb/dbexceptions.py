# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import exceptions

class Error(exceptions.StandardError):
  pass

class Warning(exceptions.StandardError):
  pass

class InterfaceError(Error):
  pass

class DatabaseError(Error):
  pass

class InternalError(DatabaseError):
  pass

class OperationalError(DatabaseError):
  pass

class ProgrammingError(DatabaseError):
  pass

class IntegrityError(DatabaseError):
  pass

class DataError(DatabaseError):
  pass

class NotSupportedError(DatabaseError):
  pass
