"""Simple load test for Vitess."""

import time
import random
import threading

from vtdb import vtgate_client
from vtdb import grpc_vtgate_client  # pylint: disable=unused-import

# Need 2 connections because we're multi-threading.
conn1 = vtgate_client.connect('grpc', 'vtgate-zone1:15991', 10)
conn2 = vtgate_client.connect('grpc', 'vtgate-zone1:15991', 10)


def func_loop(func, interval):
  """Call func() in a loop at the given interval."""
  while True:
    start_time = time.clock()
    try:
      func()
    except Exception as e:
      print(e)
    remaining_time = interval - (time.clock() - start_time)
    if remaining_time > 0: time.sleep(remaining_time)


def select_loop():
  """Execute queries on slaves."""
  cursor = conn1.cursor(tablet_type='replica', keyspace='main')
  def select_func():
    cursor.execute('SELECT * FROM messages WHERE page=:page LIMIT 1',
                   {'page': random.randint(1,100)})
  func_loop(select_func, 1.0/100.0)


def insert_loop():
  """Execute updates on the master."""
  cursor = conn2.cursor(tablet_type='master', keyspace='main', writable=True)
  def insert_func():
    now = time.time()
    page = random.randint(1,100)
    cursor.begin()
    try:
      cursor.execute('DELETE FROM messages WHERE page=:page AND time_created_ns < :time_created_ns',
                     {'page': page, 'time_created_ns': int((now - 10) * 1e9)})
      cursor.execute('INSERT INTO messages (page, time_created_ns, message)'
                     ' VALUES (:page, :time_created_ns, :message)',
                     {'page': page, 'time_created_ns': int(now*1e9), 'message': 'hi'})
      cursor.commit()
    except:
      cursor.rollback()
      raise
  func_loop(insert_func, 1.0/25.0)


# Start both loops.
threading.Thread(target=select_loop).start()
insert_loop()

