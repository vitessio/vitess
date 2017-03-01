"""Main python file."""

import argparse
import os
import time
import json

from flask import Flask

from vtdb import vtgate_client

# Register gRPC protocol.
from vtdb import grpc_vtgate_client  # pylint: disable=unused-import

app = Flask(__name__)

# conn is the connection to vtgate.
conn = None
keyspace = None


@app.route('/')
def index():
  return app.send_static_file('index.html')


@app.route('/page/<int:page>')
def view(page):
  _ = page
  return app.send_static_file('index.html')


@app.route('/lrange/guestbook/<int:page>')
def list_guestbook(page):
  """Read the list from a replica."""
  cursor = conn.cursor(tablet_type='replica', keyspace=keyspace)

  cursor.execute(
      'SELECT message, time_created_ns FROM messages WHERE page=:page'
      ' ORDER BY time_created_ns',
      {'page': page})
  entries = [row[0] for row in cursor.fetchall()]
  cursor.close()

  return json.dumps(entries)


@app.route('/rpush/guestbook/<int:page>/<value>')
def add_entry(page, value):
  """Insert a row on the master."""
  cursor = conn.cursor(tablet_type='master', keyspace=keyspace, writable=True)

  cursor.begin()
  cursor.execute(
      'INSERT INTO messages (page, time_created_ns, message)'
      ' VALUES (:page, :time_created_ns, :message)',
      {
          'page': page,
          'time_created_ns': int(time.time() * 1e9),
          'message': value,
      })
  cursor.commit()

  # Read the list back from master (critical read) because it's
  # important that the user sees their own addition immediately.
  cursor.execute(
      'SELECT message, time_created_ns FROM messages WHERE page=:page'
      ' ORDER BY time_created_ns',
      {'page': page})
  entries = [row[0] for row in cursor.fetchall()]
  cursor.close()

  return json.dumps(entries)


@app.route('/env')
def env():
  return json.dumps(dict(os.environ))


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Run guestbook app')
  parser.add_argument('--port', help='Port', default=8080, type=int)
  parser.add_argument('--cell', help='Cell', default='test', type=str)
  parser.add_argument(
      '--keyspace', help='Keyspace', default='test_keyspace', type=str)
  parser.add_argument(
      '--timeout', help='Connect timeout (s)', default=10, type=int)
  parser.add_argument(
      '--vtgate_port', help='Vtgate Port', default=15991, type=int)
  guestbook_args = parser.parse_args()

  # Get vtgate service address from Kubernetes DNS.
  addr = 'vtgate-%s:%d' % (guestbook_args.cell, guestbook_args.vtgate_port)

  # Connect to vtgate.
  conn = vtgate_client.connect('grpc', addr, guestbook_args.timeout)

  keyspace = guestbook_args.keyspace

  app.run(host='0.0.0.0', port=guestbook_args.port, debug=True)
