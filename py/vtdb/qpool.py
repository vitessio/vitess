# Implements a query thread pool, responsible for executing
# queries/fetches across multiple database cursors in parallel and returning the
# results. It implements one query per cursor, one query for all cursors, as well as
# fetchmany and fetchall equivalents.

from Queue import Queue
from threading import Thread
import logging


class ExecuteQueryQueueItem(object):
  def __init__(self, cursor, execute_args, execute_kargs, result_queue):
    self.cursor = cursor
    self.execute_args = execute_args
    self.execute_kargs = execute_kargs
    self.result_queue = result_queue

  def __str__(self):
    return "ExecuteQueryQueueItem: %s %s %s %s" % (self.cursor, self.execute_args, self.execute_kargs, self.result_queue)

class FetchQueryQueueItem(object):
  def __init__(self, cursor, result_queue):
    self.cursor = cursor
    self.result_queue = result_queue

  def __str__(self):
    return "FetchQueryQueueItem: %s %s" % (self.cursor, self.result_queue)

class FetchManyQueryQueueItem(object):
  def __init__(self, cursor, result_queue, fetch_size):
    self.cursor = cursor
    self.result_queue = result_queue
    self.fetch_size = fetch_size

  def __str__(self):
    return "FetchManyQueryQueueItem: %s %s %s" % (self.cursor, self.result_queue, self.fetch_size)

class StopQueueItem(object):
  def __init__(self, thread_name):
    self.thread_name = thread_name

class ResultQueueItem(object):
  def __init__(self, result=None, error=None):
    self.result = result
    self.error = error

  def __str__(self):
    return "ResultQueueItem: %s %s" % (self.result, self.error)

class ResultRowsQueueItem(object):
  def __init__(self, rows=None, error=None):
    if rows is None:
      self.rows = []
    else:
      self.rows = rows
    self.error = error

  def __str__(self):
    return "ResultRowsQueueItem: %s %s" % (len(self.rows), self.error)


def drain_query_queue(query_queue, thread_name):
  while True:
    qq_item = None
    try:
      try:
        try:
          qq_item = query_queue.get()
          if type(qq_item) is ExecuteQueryQueueItem:
            result = qq_item.cursor.execute(*qq_item.execute_args, **qq_item.execute_kargs)
            rq_item = ResultQueueItem(result=result)
          elif type(qq_item) is FetchQueryQueueItem:
            rows = qq_item.cursor.fetchall()
            rq_item = ResultRowsQueueItem(rows=rows)
          elif type(qq_item) is FetchManyQueryQueueItem:
            rows = qq_item.cursor.fetchmany(qq_item.fetch_size)
            rq_item = ResultRowsQueueItem(rows=rows)
          elif type(qq_item) is StopQueueItem:
            if qq_item.thread_name == thread_name:
              break
            else:
              query_queue.put(qq_item)
              continue
          else:
            rq_item = ResultQueueItem(error=Exception("unknown item: %s" % qq_item))
        except Exception as e:
          if isinstance(qq_item, (FetchManyQueryQueueItem, FetchQueryQueueItem)):
            rq_item = ResultRowsQueueItem(error=e)
          else:
            rq_item = ResultQueueItem(error=e)
        qq_item.result_queue.put(rq_item)
        del qq_item
      except Exception as e:
        logging.exception("error in drain_query_queue")
    finally:
      if 'qq_item' in vars():
        del qq_item

class DebugQueue:
  def __init__(self, *args):
    self.queue = Queue(*args)

  def put(self, qq_item):
    logging.debug("vt query put %s", qq_item)
    return self.queue.put(qq_item)

  def get(self):
    qq_item = self.queue.get()
    logging.debug("vt query get %s", qq_item)
    return qq_item

class QueryThreadPool(object):
  def __init__(self, thread_pool_size=1):
    self.query_queue = Queue()
    self.thread_pool = []
    for x in xrange(thread_pool_size):
      thread_name = 'vt-qtp-%s' % x
      t = Thread(target=drain_query_queue, name=thread_name,
             args=(self.query_queue, thread_name))
      self.thread_pool.append(t)
      t.setDaemon(True)
      t.start()

  def shutdown(self):
    for t in self.thread_pool:
      self.query_queue.put(StopQueueItem(t.getName()))
    for t in self.thread_pool:
      t.join()

  def execute_query(self, cursor_list, *execute_args, **execute_kargs):
    result_queue = Queue()
    for cursor in cursor_list:
      self.query_queue.put(ExecuteQueryQueueItem(cursor, execute_args, execute_kargs, result_queue))
    result_list = []
    for i in xrange(len(cursor_list)):
      result_list.append(result_queue.get())
    return result_list

  def execute_vector_query(self, cursor_list, query_list, bind_vars_list):
    result_queue = Queue()
    for cursor, query, bind_vars in zip(cursor_list, query_list, bind_vars_list):
      self.query_queue.put(
        ExecuteQueryQueueItem(cursor, (query, bind_vars), {}, result_queue))
    result_list = []
    for i in xrange(len(cursor_list)):
      result_list.append(result_queue.get())
    return result_list

  def fetchall_results(self, cursor_list):
    result_queue = Queue()
    for cursor in cursor_list:
      self.query_queue.put(FetchQueryQueueItem(cursor, result_queue))
    result_list = []
    for i in xrange(len(cursor_list)):
      result_list.append(result_queue.get())
    return result_list

  def fetchmany_results_generator(self, cursor_list, fetch_size=100):
    result_queue = Queue()
    for cursor in cursor_list:
      qq_item = FetchManyQueryQueueItem(cursor, result_queue, fetch_size)
      self.query_queue.put(qq_item)
    for i in xrange(len(cursor_list)):
      qq_item = result_queue.get()
      yield qq_item
