#!/usr/bin/env python

import collections
import datetime
import difflib
import heapq
import itertools
import json
import logging
import optparse
import os
import pprint
import Queue
import re
import sys
import tempfile
import threading
import time

import MySQLdb
import MySQLdb.cursors


ws = re.compile(r'\s+')
def clean(s):
  return ws.sub(' ', s).strip()


def merge_sorted(seqs, key=None):
  if key is None:
    return heapq.merge(*seqs)
  else:
    return (i[1] for i in heapq.merge(*(((key(item), item) for item in seq) for seq in seqs)))


class AtomicWriter(object):
  """AtomicWriter is a file-like object that allows you to do an
  atomic write (on close), using os.rename.
  """
  def __init__(self, filename, directory):
    self.filename = filename
    self.tempfile  = tempfile.NamedTemporaryFile(delete=False, dir=directory)

  def write(self, s):
    return self.tempfile.write(s)

  def close(self):
    self.tempfile.close()
    os.rename(self.tempfile.name, self.filename)

  def __enter__(self):
    return self

  def __exit__(self, *args, **kwargs):
    self.close()
    return False


def sql_tuple_comparison(tablename, columns, column_name_prefix=''):
  """Tuple comparison has the semantics I need, but it confuses
  MySQL's optimizer. This code returns SQL equivalent to tuple
  comparison.
  """
  head = columns[0]

  if len(columns) == 1:
    return '%(tablename)s.%(column)s > %%(%(column_name_prefix)s%(column)s)s' % {
        'column': head,
        'tablename': tablename,
        'column_name_prefix': column_name_prefix}

  return """%(tablename)s.%(column)s > %%(%(column_name_prefix)s%(column)s)s or
            (%(tablename)s.%(column)s = %%(%(column_name_prefix)s%(column)s)s and (%(rec)s))""" % {
                'column': head,
                'rec': sql_tuple_comparison(tablename, columns[1:], column_name_prefix),
                'tablename': tablename,
                'column_name_prefix': column_name_prefix}


class Row(object):
  def __init__(self, pk, data):
    self.data = data
    self.pk = tuple(self.data[k] for k in pk)

  def __getitem__(self, key):
    return self.data[key]

  def __eq__(self, other):
    return self.data == other.data

  def __ne__(self, other):
    return not(self == other)

  def __repr__(self):
    return repr(self.data)


def sorted_row_list_difference(expected, actual):
  """Finds elements in only one or the other of two, sorted input lists.

  Returns a three-element tuple of lists. The first list contains
  those elements in the "expected" list but not in the "actual"
  list, the second contains those elements in the "actual" list but
  not in the "expected" list, the third contains rows that are
  different despite having the same primary key.
  """
  # adapted from unittest.
  i = j = 0
  missing = []
  unexpected = []
  different = []
  while True:
    try:
      e, a = expected[i], actual[j]
      if e.pk < a.pk:
        missing.append(e)
        i += 1
      elif e.pk > a.pk:
        unexpected.append(a)
        j += 1
      else:
        if a != e:
          different.append((a, e))
        i += 1
        j += 1
    except IndexError:
      missing.extend(expected[i:])
      unexpected.extend(actual[j:])
      break
  return missing, unexpected, different


class Datastore(object):
  """Datastore is database which expects that all queries sent to it
  will use the same primary key.
  """
  def __init__(self, connection_params, primary_key, stats=None):
    self.primary_key = primary_key
    self.stats = stats
    self.connection_params = dict((str(key), value) for key, value in connection_params.items())
    self.dbname = connection_params['db']
    self._cursor = None

  @property
  def cursor(self):
    # NOTE(szopa): This is a property so that connection object is
    # created in the first thread that tries to access it.
    if self._cursor is None:
      self._cursor = MySQLdb.connect(
        cursorclass=MySQLdb.cursors.DictCursor,
        **self.connection_params).cursor()
    return self._cursor

  def query(self, sql, params, retries=3):
    start = time.time()
    self.cursor.execute(sql, params)
    if self.stats:
      self.stats.update(self.dbname, start)
    return [Row(self.primary_key, item) for item in self.cursor.fetchall()]


class DatastoreThread(threading.Thread):
  def __init__(self, datastore, retries=3):
    super(DatastoreThread, self).__init__()
    self.datastore = datastore
    self.in_queue = Queue.Queue(maxsize=1)
    self.out_queue = Queue.Queue(maxsize=1)
    self.retries = retries
    self.daemon = True
    self.start()

  def run(self):
    while True:
      sql, params = self.in_queue.get()
      i = 0
      while True:
        try:
          self.out_queue.put(self.datastore.query(sql, params))
          break
        except MySQLdb.OperationalError as e:
          i += 1
          logging.exception("Error reading from %s", self.datastore.dbname)
          if i < self.retries:
            self.out_queue.put(e)
            break
          time.sleep(1)
        except Exception as e:
          logging.exception("Unexpected exception while reading from %s", self.datastore.dbname)
          self.out_queue.put(e)
          break

  def query(self, sql, params):
    self.in_queue.put((sql, params))

  def get(self):
    d = self.out_queue.get()
    if isinstance(d, Exception):
      raise d
    return d


class MultiDatastore(object):
  """MultiDatastore gathers results from a list of Datastores. Each
  datastore is queried in a separate thread.
  """
  def __init__(self, connection_params_list, primary_key, nickname, stats=None):
    self.nickname = nickname
    self.stats = stats
    self.threads = [DatastoreThread(Datastore(params, primary_key)) for params in connection_params_list]

  def query(self, sql, params):
    """Query all the child datastores in parallel.
    """
    start = time.time()
    for thread in self.threads:
      thread.query(sql, params)
    data = [thread.get() for thread in self.threads]
    sdata = list(merge_sorted(data, key=lambda r: r.pk))
    if self.stats:
      self.stats.update(self.nickname, start)
    return sdata


class Mismatch(Exception):
  def __init__(self, missing, unexpected, different):
    self.missing, self.unexpected, self.different = missing, unexpected, different

  def dict_diff(self, left, right):
    return '\n'.join(difflib.ndiff(
        pprint.pformat(left).splitlines(),
        pprint.pformat(right).splitlines()))

  def __str__(self):
    data = []
    if self.missing:
      data.append("Missing in destination:\n%s" % pprint.pformat(self.missing))
    if self.unexpected:
      data.append("Unexpected in destination:\n%s" % pprint.pformat(self.unexpected))
    if self.different:
      data.append("Different:\n%s" % '\n'.join(self.dict_diff(*d) for d in self.different))

    return '\n'.join(data)


class Stats(object):

  def __init__(self, interval=0):
    self.interval = interval
    self.clear()

  def clear(self):
    self.times = collections.defaultdict(float)
    self.items = 0
    self.clear_local()

  def clear_local(self):
    self.local_times = collections.defaultdict(float)
    self.local_items = 0
    self.last_flush = time.time()

  def update(self, key, from_time, items=0):
    logging.debug("update: key: %s, from_time: %s, items: %s", key, from_time, items)
    # Items are incremented only by 'total'
    t = time.time() - from_time
    self.local_times[key] += t
    self.times[key] += t
    self.items += items
    self.local_items += items

  def maybe_print_local(self, force=False):
    if self.interval == 0:
      return
    if force or (time.time() - self.last_flush) >= self.interval:
      try:
        total = self.local_times.pop('total')
      except KeyError:
        pass
      else:
        data = ["total speed: %0.2f items/s" % (self.local_items / total)]
        data.extend("\t%s: %0.2f%%" % (k, (v * 100) / total) for k, v in self.local_times.items())
        logging.info('\t'.join(data))
      self.clear_local()

  def print_total(self):
    try:
      total = self.times.pop('total')
    except KeyError:
      logging.info('No stats: no work was necessary.')
    else:
      data = ["(FINAL) total speed: %0.2f items/s" % (self.items / total)]
      data.extend("\t%s: %0.2f%%" % (k, (v * 100) / total) for k, v in self.times.items())
      logging.info('\t'.join(data))


class Checker(object):
  def __init__(self, configuration, table, directory, batch_count=0, blocks=1, ratio=1.0, block_size=16384, logging_level=logging.INFO, stats_interval=1, temp_directory=None):
    self.table_name = table

    table_data = configuration['tables'][table]
    self.primary_key = table_data['pk']
    self.columns = table_data['columns']

    self.current_pk = dict((k, 0) for k in self.primary_key)
    if batch_count != 0:
      self.batch_size = batch_count
    else:
      try:
        rows_per_block = float(block_size) / table_data.get('avg_row_length', 0)
      except ZeroDivisionError:
        rows_per_block = 20
      self.batch_size = int(rows_per_block * ratio * blocks)

    self.iterations = 0
    self.temp_directory = temp_directory
    self.checkpoint_file = os.path.join(directory, table + '.json')
    self.done = False
    try:
      self.restore_checkpoint()
    except IOError:
      pass

    keyrange = configuration["keyrange"]
    keyspace_sql_parts = []
    if keyrange.get('start') or keyrange.get('end'):
      if keyrange.get('start'):
        keyspace_sql_parts.append("keyspace_id >= %s and" % keyrange.get('start'))
      if keyrange.get('end'):
        keyspace_sql_parts.append("keyspace_id < %s and" % keyrange.get('end'))

    self.destination_sql = """
           select
             %(columns)s
           from %(table_name)s use index (primary)
           where
             %(range_sql)s
           order by %(pk_columns)s limit %%(limit)s""" % {
               'table_name': self.table_name,
               'columns': ', '.join(self.columns),
               'pk_columns': ', '.join(self.primary_key),
               'range_sql': sql_tuple_comparison(self.table_name, self.primary_key)}

    # Almost like destination SQL except for the keyspace_sql clause.
    self.last_source_sql = """
           select
             %(columns)s
           from %(table_name)s use index (primary)
           where %(keyspace_sql)s
             %(range_sql)s
           order by %(pk_columns)s limit %%(limit)s""" % {
               'table_name': self.table_name,
               'keyspace_sql': ' '.join(keyspace_sql_parts),
               'columns': ', '.join(self.columns),
               'pk_columns': ', '.join(self.primary_key),
               'range_sql': sql_tuple_comparison(self.table_name, self.primary_key)}

    self.source_sql = """
           select
             %(columns)s
           from %(table_name)s use index (primary)
           where %(keyspace_sql)s
             ((%(min_range_sql)s) and not (%(max_range_sql)s))
           order by %(pk_columns)s""" % {
               'table_name': self.table_name,
               'keyspace_sql': ' '.join(keyspace_sql_parts),
               'columns': ', '.join(self.columns),
               'pk_columns': ', '.join(self.primary_key),
               'min_range_sql': sql_tuple_comparison(self.table_name, self.primary_key),
               'max_range_sql': sql_tuple_comparison(self.table_name, self.primary_key, column_name_prefix='max_')}

    self.stats = Stats(interval=stats_interval)
    self.destination = Datastore(configuration['destination'], self.primary_key, stats=self.stats)
    self.sources = MultiDatastore(configuration['sources'], self.primary_key, 'all-sources', stats=self.stats)

    logging.basicConfig(level=logging_level)
    logging.debug("destination sql template: %s", clean(self.destination_sql))
    logging.debug("source sql template: %s", clean(self.source_sql))

  def get_pk(self, row):
    return dict((k, row[k]) for k in self.primary_key)

  def get_data(self):
    while True:
      start = time.time()
      params = dict(self.current_pk)
      params['limit'] = self.batch_size

      logging.debug("destination execute: %r", clean(self.destination_sql % params))
      destination_data = self.destination.query(self.destination_sql, params)
      if destination_data:
        new_current_pk = self.get_pk(destination_data[-1])

        source_params = dict(self.current_pk)
        for k, v in new_current_pk.items():
          source_params['max_' + k] = v

        logging.debug("source execute: %r", clean(self.source_sql % source_params))
        source_data = self.sources.query(self.source_sql, source_params)
        self.current_pk = new_current_pk
      else:
        logging.debug("no more items in destination, trying an unbound query in source")
        source_data = self.sources.query(self.last_source_sql, params)
        # NOTE(szopa): I am setting self.current_pk event though it
        # probably won't be used (run will raise an exception after
        # the first suspicious batch).
        try:
          self.current_pk = self.get_pk(source_data[-1])
        except IndexError:
          break
      if not source_data and not destination_data:
        break
      self.stats.update('selects', start)
      yield destination_data, source_data

  def restore_checkpoint(self):
    with open(self.checkpoint_file) as fi:
      checkpoint = json.load(fi)
    self.current_pk, self.done = checkpoint['current_pk'], checkpoint['done']

  def checkpoint(self, done=False):
    start = time.time()
    data = {'current_pk': self.current_pk,
            'done': done,
            'timestamp': str(datetime.datetime.now())}
    with AtomicWriter(self.checkpoint_file, self.temp_directory) as fi:
      json.dump(data, fi)
    self.stats.update('checkpoint', start)

  def _run(self):
    if self.done:
      return
    start = time.time()
    for dd, sd in self.get_data():
      self.iterations += 1
      self.checkpoint()
      start_processing = time.time()
      missing, unexpected, different = sorted_row_list_difference(sd, dd)
      self.stats.update('processing', start_processing)
      self.stats.update('total', start, len(sd))
      if any([missing, unexpected, different]):
        raise Mismatch(missing, unexpected, different)
      start = time.time()
      self.stats.maybe_print_local()

    self.checkpoint(done=True)
    self.stats.print_total()

  def run(self):
    try:
      self._run()
    except Mismatch as e:
      print e

def main():
  parser = optparse.OptionParser()
  parser.add_option('--batch-count',
                    type='int', default=1000, dest='batch_count',
                    help='process this many rows in one batch')
  parser.add_option('--stats', type='int',
                    default=0, dest='stats',
                    help='Print stats every n seconds.')
  parser.add_option('-c', '--checkpoint-directory',
                    dest='checkpoint_directory', type='string',
                    help='Directory to store checkpoints.',
                    default='.')
  parser.add_option('-r', '--ratio', dest='ratio',
                    type='float', default=1.0,
                    help='Assumed block fill ratio.')
  parser.add_option('-b', '--blocks', dest='blocks',
                    type='float', default=3,
                    help='Try to send this many blocks in one commit.')
  parser.add_option('--block-size',
                    type='int', default=16384, dest='block_size',
                    help='Assumed size of a block')

  (options, args) = parser.parse_args()
  config_file, table = args

  with open(config_file) as fi:
    config = json.load(fi)

  checker = Checker(config, table, options.checkpoint_directory,
                    stats_interval=options.stats, batch_count=options.batch_count,
                    block_size=options.block_size, ratio=options.ratio,
                    temp_directory=options.checkpoint_directory)
  checker.run()

if __name__ == '__main__':
  main()
