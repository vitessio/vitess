import logging
import time

from vtdb import vtdb_logger

INITIAL_DELAY_MS = 5
NUM_RETRIES = 3
MAX_DELAY_MS = 100
BACKOFF_MULTIPLIER = 2


def exponential_backoff_retry(
    retry_exceptions,
    initial_delay_ms=INITIAL_DELAY_MS,
    num_retries=NUM_RETRIES,
    backoff_multiplier=BACKOFF_MULTIPLIER,
    max_delay_ms=MAX_DELAY_MS):
  """decorator for exponential backoff retry

  Log and raise exception if unsuccessful
  Do not retry while in a session

  retry_exceptions: tuple of exceptions to check
  initial_delay_ms: initial delay between retries in ms
  num_retries: number max number of retries
  backoff_multipler: multiplier for each retry e.g. 2 will double the retry delay
  max_delay_ms: upper bound on retry delay
  """
  def decorator(method):
    def wrapper(self, *args, **kargs):
      attempt = 0
      delay = initial_delay_ms
      while True:
        try:
          return method(self, *args, **kargs)
        except retry_exceptions as e:
          attempt += 1
          if attempt > num_retries or self.session:
            vtdb_logger.get_logger().vtgatev2_exception(e)
            raise e
          logging.error("retryable error: %s, retrying in %d ms, attempt %d of %d", e, delay, attempt, num_retries)
          time.sleep(delay/1000.0)
          delay *= backoff_multiplier
          delay = min(max_delay_ms, delay)
    return wrapper
  return decorator
