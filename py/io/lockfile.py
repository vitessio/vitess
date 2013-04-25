# Quick exclusive lock for ensuring two processes don't stomp each other.
#
# The only drawback here is that this leaves a lock turdfile that doesn't
# get cleaned up. Practically speaking this shouldn't be a problem, but it
# doesn't feel entirely satisfying.
#
# This lock has the handy property that if the process dies for any reason,
# the lock is released.

import errno
import fcntl
import logging
import os

class FLock(object):
  def __init__(self, path):
    self._path = path
    self._fd = None

  def acquire(self, blocking=True):
    if self._fd:
      raise ValueError('lock already held', self._path)

    self._fd = os.open(self._path, os.O_CREAT | os.O_WRONLY)
    # use flock because it is held through a fork()
    try:
      # do a non-blocking check for the sole purpose of showing a warning
      # message to the hapless user
      fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
      logging.debug('acquired lock (nb) %s', self._path)
      if not blocking:
        return True
      return
    except IOError as e:
      if e[0] in (errno.EACCES, errno.EAGAIN):
        if not blocking:
          os.close(self._fd)
          self._fd = None
          return False
        # the lock is held, dump a warning and wait
        logging.debug('waiting for lock %s', self._path)
      else:
        raise
    fcntl.flock(self._fd, fcntl.LOCK_EX)
    logging.debug('acquired lock %s', self._path)

  def release(self):
    if not self._fd:
      raise ValueError('lock not held', self._path)

    # using flock, calling unlock on any duplicate of the original fd
    # will cause the lock to be released, which is what we want.
    fcntl.flock(self._fd, fcntl.LOCK_UN)

    # NOTE: don't remove the lockfile, that will cause the lock to get into an
    # inconsistent state, just close it.
    os.close(self._fd)
    self._fd = None
