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
import cPickle as pickle
import pprint
import Queue
import re
import sys
import tempfile
import threading
import time
import urlparse

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

def parse_database_url(url, password_map):
  if not url.startswith('mysql'):
    url = 'mysql://' + url
  url = 'http' + url[len('mysql'):]
  parsed = urlparse.urlparse(url)
  params = {'user': parsed.username,
            'host': parsed.hostname,
            'db': parsed.path[1:]}
  if parsed.username:
    params['user'] = parsed.username
  if parsed.password:
    params['passwd'] = parsed.password
  elif parsed.username and parsed.username in password_map:
    params['passwd'] = password_map[parsed.username][0]
  if parsed.port:
    params['port'] = parsed.port
  params.update(dict(urlparse.parse_qsl(parsed.query)))
  return params


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


def sorted_row_list_difference(expected, actual, key_length):
  """Finds elements in only one or the other of two, sorted input lists.

  Returns a three-element tuple of lists. The first list contains
  those elements in the "expected" list but not in the "actual"
  list, the second contains those elements in the "actual" list but
  not in the "expected" list, the third contains rows that are
  different despite having the same primary key.
  """
  # adapted from unittest.
  missing = []
  unexpected = []
  different = []

  expected, actual = iter(expected), iter(actual)
  enext = expected.next
  anext = actual.next
  try:
    e, a = enext(), anext()
    while True:
      if a == e:
        e, a = enext(), anext()
        continue

      ekey, akey = e[:key_length], a[:key_length]

      if ekey < akey:
        missing.append(e)
        e = enext()
      elif ekey > akey:
        unexpected.append(a)
        a = anext()
      else:
        different.append((a, e))
        e, a = enext(), anext()
  except StopIteration:
    missing.extend(expected)
    unexpected.extend(actual)

  return missing, unexpected, different


class Datastore(object):
  """Datastore is database which expects that all queries sent to it
  will use the same primary key.
  """
  def __init__(self, connection_params, stats=None):
    self.stats = stats
    self.connection_params = dict((str(key), value) for key, value in connection_params.items())
    self.dbname = connection_params['db']
    self._cursor = None

  @property
  def cursor(self):
    # NOTE(szopa): This is a property so that connection object is
    # created in the first thread that tries to access it.
    if self._cursor is None:
      self._cursor = MySQLdb.connect(**self.connection_params).cursor()
    return self._cursor

  def query(self, sql, params, retries=3):
    start = time.time()
    self.cursor.execute(sql, params)
    if self.stats:
      self.stats.update(self.dbname, start)
    return self.cursor.fetchall()


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
  def __init__(self, connection_params_list, nickname, stats=None):
    self.nickname = nickname
    self.stats = stats
    self.threads = [DatastoreThread(Datastore(params)) for params in connection_params_list]

  def query(self, sql, params):
    """Query all the child datastores in parallel.
    """
    start = time.time()
    for thread in self.threads:
      thread.query(sql, params)
    data = [thread.get() for thread in self.threads]
    return data


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

    return '\n'.join(data) + '\n'


class Stats(object):

  def __init__(self, interval=0, name=""):
    self.lock = threading.Lock()
    self.interval = interval
    self.name = name
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
    with self.lock:
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
        data = [self.name, "total speed: %0.2f items/s" % (self.local_items / total)]
        data.extend("\t%s: %0.2f%%" % (k, (v * 100) / total) for k, v in self.local_times.items())
        logging.info('\t'.join(data))
      self.clear_local()

  def print_total(self):
    try:
      total = self.times.pop('total')
    except KeyError:
      logging.info('No stats: no work was necessary.')
    else:
      data = [self.name, "(FINAL) total speed: %0.2f items/s" % (self.items / total)]
      data.extend("\t%s: %0.2f%%" % (k, (v * 100) / total) for k, v in self.times.items())
      logging.info('\t'.join(data))


class Checker(object):

  def __init__(self, destination_url, sources_urls, table, directory='.',
               source_column_map=None, source_table_name=None, source_force_index_pk=True,
               destination_force_index_pk=True,
               keyrange={}, batch_count=0, blocks=1, ratio=1.0, block_size=16384,
               logging_level=logging.INFO, stats_interval=1, temp_directory=None, password_map_file=None):
    self.table_name = table
    if source_table_name is None:
      self.source_table_name = self.table_name
    else:
      self.source_table_name = source_table_name

    if password_map_file:
      with open(password_map_file, "r") as f:
        password_map = json.load(f)
    else:
      password_map = {}

    self.table_data = self.get_table_data(table, parse_database_url(destination_url, password_map))
    self.primary_key = self.table_data['pk']

    if source_column_map:
      self.source_column_map = source_column_map
    else:
      self.source_column_map = {}

    columns = self.table_data['columns']

    for k in self.primary_key:
      columns.remove(k)
    self.columns = self.primary_key + columns
    self.source_columns = [self.source_column_map.get(c, c) for c in self.columns]
    self.source_primary_key = [self.source_column_map.get(c, c) for c in self.primary_key]
    self.pk_length = len(self.primary_key)
    (self.batch_count, self.block_size,
     self.ratio, self.blocks) = batch_count, block_size, ratio, blocks

    self.calculate_batch_size()

    self.current_pk = dict((k, 0) for k in self.primary_key)

    self.iterations = 0
    self.temp_directory = temp_directory
    self.checkpoint_file = os.path.join(directory, table + '.pickle')
    self.mismatches_file = os.path.join(directory, table + '_mismatches.txt')
    self.done = False
    try:
      self.restore_checkpoint()
    except IOError:
      pass
    if source_force_index_pk:
      source_use_index = 'use index (primary)'
    else:
      source_use_index = ''
    if destination_force_index_pk:
      destination_use_index = 'use index (primary)'
    else:
      destination_use_index = ''

    keyspace_sql_parts = []
    if keyrange.get('start') or keyrange.get('end'):
      if keyrange.get('start'):
        keyspace_sql_parts.append("keyspace_id >= %s and" % keyrange.get('start'))
      if keyrange.get('end'):
        keyspace_sql_parts.append("keyspace_id < %s and" % keyrange.get('end'))

    self.destination_sql = """
           select
             %(columns)s
           from %(table_name)s %(use_index)s
           where
             %(range_sql)s
           order by %(pk_columns)s limit %%(limit)s""" % {
               'table_name': self.table_name,
               'use_index': destination_use_index,
               'columns': ', '.join(self.columns),
               'pk_columns': ', '.join(self.primary_key),
               'range_sql': sql_tuple_comparison(self.table_name, self.primary_key)}

    # Almost like destination SQL except for the keyspace_sql clause.
    self.last_source_sql = """
           select
             %(columns)s
           from %(table_name)s %(use_index)s
           where %(keyspace_sql)s
             (%(range_sql)s)
           order by %(pk_columns)s limit %%(limit)s""" % {
               'table_name': self.source_table_name,
               'use_index': source_use_index,
               'keyspace_sql': ' '.join(keyspace_sql_parts),
               'columns': ', '.join(self.source_columns),
               'pk_columns': ', '.join(self.source_primary_key),
               'range_sql': sql_tuple_comparison(self.source_table_name, self.source_primary_key)}

    self.source_sql = """
           select
             %(columns)s
           from %(table_name)s %(use_index)s
           where %(keyspace_sql)s
             ((%(min_range_sql)s) and not (%(max_range_sql)s))
           order by %(pk_columns)s""" % {
               'table_name': self.source_table_name,
               'use_index': source_use_index,
               'keyspace_sql': ' '.join(keyspace_sql_parts),
               'columns': ', '.join(self.source_columns),
               'pk_columns': ', '.join(self.source_primary_key),
               'min_range_sql': sql_tuple_comparison(self.source_table_name, self.source_primary_key),
               'max_range_sql': sql_tuple_comparison(self.source_table_name, self.source_primary_key, column_name_prefix='max_')}

    self.stats = Stats(interval=stats_interval, name=self.table_name)
    self.destination = Datastore(parse_database_url(destination_url, password_map), stats=self.stats)
    self.sources = MultiDatastore([parse_database_url(s, password_map) for s in sources_urls], 'all-sources', stats=self.stats)

    logging.basicConfig(level=logging_level)
    logging.debug("destination sql template: %s", clean(self.destination_sql))
    logging.debug("source sql template: %s", clean(self.source_sql))

  def get_table_data(self, table_name, params):
    table = {'columns': [], 'pk': []}
    conn = MySQLdb.connect(**params)
    cursor = conn.cursor()
    cursor.execute("SELECT avg_row_length FROM information_schema.tables WHERE table_schema = %s AND table_name = %s", (params['db'], table_name))
    table['avg_row_length'] = cursor.fetchone()[0]

    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY table_name, ordinal_position", (params['db'], table_name))
    for row in cursor.fetchall():
      table['columns'].append(row[0])

    cursor.execute("select column_name FROM information_schema.key_column_usage WHERE table_schema=%s AND constraint_name='PRIMARY' AND table_name = %s ORDER BY table_name, ordinal_position", (params['db'], table_name))
    for row in cursor.fetchall():
      table['pk'].append(row[0])
    return table

  def calculate_batch_size(self):
    if self.batch_count != 0:
      self.batch_size = self.batch_count
    else:
      try:
        rows_per_block = float(self.block_size) / self.table_data.get('avg_row_length', 0)
      except ZeroDivisionError:
        rows_per_block = 20
      self.batch_size = int(rows_per_block * self.ratio * self.blocks)

  def get_pk(self, row):
    return dict((k, v) for k, v in zip(self.primary_key, row))

  def _run(self):
    # initialize destination_in_queue, sources_in_queue, merger_in_queue, comparer_in_queue, comparare_out_queue
    self.destination_in_queue = Queue.Queue(maxsize=3)
    self.sources_in_queue = Queue.Queue(maxsize=3)
    self.merger_comparer_in_queue = Queue.Queue(maxsize=3)
    self.merger_comparer_out_queue = Queue.Queue(maxsize=3)

    # start destination, sources, merger, comparer
    threads = []
    for worker_name in ['destination_worker', 'sources_worker', 'merger_comparer_worker']:
      worker = getattr(self, worker_name)
      t = threading.Thread(target=worker, name=worker_name)
      t.daemon = True
      threads.append(t)
      t.start()
    self.destination_in_queue.put((self.current_pk, None))

    start = time.time()
    while True:
      # get error from the comparer out-queue, raise if it isn't None.

      error_or_done, processed_rows = self.merger_comparer_out_queue.get()
      self.stats.update('total', start, processed_rows)
      start = time.time()
      self.stats.maybe_print_local()
      if isinstance(error_or_done, Mismatch):
        self.handle_mismatch(error_or_done)
        continue

      if error_or_done:
        self.destination_in_queue.put((None, True))
      if error_or_done is True:
        self.stats.print_total()
        return
      elif error_or_done is not None:
        raise error_or_done

  def handle_mismatch(self, mismatch):
    with open(self.mismatches_file, 'a') as fi:
      fi.write(str(mismatch))

  def destination_worker(self):
    while True:
      start_pk, done = self.destination_in_queue.get()
      start = time.time()
      if done:
        self.sources_in_queue.put((None, None, None, True))
        return

      params = {'limit': self.batch_size}
      params.update(start_pk)

      # query the destination -> data
      try:
        destination_data = self.destination.query(self.destination_sql, params)
      except MySQLdb.ProgrammingError as e:
        self.sources_in_queue.put((None, None, None, e))
        return

      try:
        end_pk = self.get_pk(destination_data[-1])
      except IndexError:
        # There's no more data in the destination. The next object we
        # get from the in-queue is going to be put there by _run or is
        # going to be an error.
        self.sources_in_queue.put((start_pk, None, [], None))
      else:
        # put the (range-pk, data) on the sources in-queue
        self.sources_in_queue.put((start_pk, end_pk, destination_data, None))
        self.destination_in_queue.put((end_pk, None))
      self.stats.update('destination', start)


  def sources_worker(self):
    while True:
      # get (range-pk, data) from the sources in-queue
      (start_pk, end_pk, destination_data, error_or_done) = self.sources_in_queue.get()
      start = time.time()
      if error_or_done:
        self.merger_comparer_in_queue.put((None, None, error_or_done))
        return

      # query the sources -> sources_data
      params = dict((self.source_column_map.get(k, k), v) for k, v in start_pk.items())

      if destination_data:
        for k, v in end_pk.items():
          params['max_' + self.source_column_map.get(k, k)] = v
        sources_data = self.sources.query(self.source_sql, params)
      else:
        params['limit'] = self.batch_size
        sources_data = self.sources.query(self.last_source_sql, params)
      # put (sources_data, data, done) on the merger in-queue
      done = not (sources_data or destination_data)
      self.stats.update('sources', start)
      self.merger_comparer_in_queue.put((destination_data, sources_data, done))

  def merger_comparer_worker(self):
    while True:
      destination_data, sources_data, error_or_done = self.merger_comparer_in_queue.get()
      start = time.time()

      if error_or_done:
        self.merger_comparer_out_queue.put((error_or_done, 0))
        return
      # No more data in both the sources and the destination, we are
      # done.
      if destination_data == [] and not any(len(s) for s in sources_data):
        self.merger_comparer_out_queue.put((True, 0))
        return

      merged_data = heapq.merge(*sources_data)

      try:
        last_pk = self.get_pk(destination_data[-1])
      except IndexError:
        # Only sources data: short-circuit
        merged_data = list(merged_data)
        last_pk = self.get_pk(merged_data[-1])
        missing, unexpected, different = merged_data, [], []
      else:
        # compare the data
        missing, unexpected, different = sorted_row_list_difference(merged_data, destination_data, self.pk_length)

      self.stats.update('comparer', start)

      # put the mismatch or None on comparer out-queue.
      if any([missing, unexpected, different]):
        self.merger_comparer_out_queue.put((Mismatch(missing, unexpected, different), len(destination_data)))
      else:
        self.merger_comparer_out_queue.put((None, len(destination_data)))

      # checkpoint
      self.checkpoint(last_pk, done=False)

  def restore_checkpoint(self):
    with open(self.checkpoint_file) as fi:
      checkpoint = pickle.load(fi)
    self.current_pk, self.done = checkpoint['current_pk'], checkpoint['done']

  def checkpoint(self, pk, done=False):
    start = time.time()
    data = {'current_pk': pk,
            'done': done,
            'timestamp': str(datetime.datetime.now())}
    with AtomicWriter(self.checkpoint_file, self.temp_directory) as fi:
      pickle.dump(data, fi)
    self.stats.update('checkpoint', start)

  def run(self):
    try:
      self._run()
    except Mismatch as e:
      print e

def get_range(start, end):
  ret = {}
  if start != "":
    ret['start'] = int(start, 16)
  if end != "":
    ret['end'] = int(end, 16)
  return ret

def main():
  parser = optparse.OptionParser()
  parser.add_option('--batch-count',
                    type='int', default=0, dest='batch_count',
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
  parser.add_option('--source-column-map',
                    dest='source_column_map', type='string',
                    help='column_in_destination:column_in_source,column_in_destination2:column_in_source2,...',
                    default='')
  parser.add_option('--source-table-name',
                    dest='source_table_name', type='string',
                    help='name of the table in sources (if different than in destination)',
                    default=None)
  parser.add_option('--no-source-force-index', dest='source_force_index', action='store_false',
                    default=True,
                    help='Do not add a "use index (primary)" to the SQL statements issued to the sources.')
  parser.add_option('--no-destination-force-index', dest='destination_force_index', action='store_false',
                    default=True,
                    help='Do not add a "use index (primary)" to the SQL statements issued to the destination.')
  parser.add_option('-b', '--blocks', dest='blocks',
                    type='float', default=3,
                    help='Try to send this many blocks in one commit.')
  parser.add_option('--block-size',
                    type='int', default=2097152, dest='block_size',
                    help='Assumed size of a block')
  parser.add_option('--start', type='string', dest='start', default='',
                    help="keyrange start (hexadecimal)")
  parser.add_option('--end', type='string', dest='end', default='',
                    help="keyrange end (hexadecimal)")
  parser.add_option('--password-map-file', type='string', default=None,
                    help="password map file")

  (options, args) = parser.parse_args()
  table, destination, sources = args[0], args[1], args[2:]

  source_column_map = {}
  if options.source_column_map:
    for pair in options.source_column_map.split(','):
      k, v = pair.split(':')
      source_column_map[k] = v

  checker = Checker(destination, sources, table, options.checkpoint_directory,
                    source_column_map=source_column_map,
                    source_force_index_pk=options.source_force_index,
                    destination_force_index_pk=options.destination_force_index,
                    source_table_name=options.source_table_name,
                    keyrange=get_range(options.start, options.end),
                    stats_interval=options.stats, batch_count=options.batch_count,
                    block_size=options.block_size, ratio=options.ratio,
                    temp_directory=options.checkpoint_directory,
                    password_map_file=options.password_map_file)
  checker.run()

if __name__ == '__main__':
  main()
