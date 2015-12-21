import os
import time
import json
import struct
import hashlib

from flask import Flask
app = Flask(__name__)

from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import vtgatev2
from vtdb import vtgate_cursor

# conn is the connection to vtgate.
conn = None

# When using the "uint64" keyspace_id type, Vitess expects big-endian encoding.
uint64 = struct.Struct('!Q')

def get_keyspace_id(page):
  """Compute the keyspace_id for a given page number.

  In this example, the keyspace_id is the first 64 bits of the MD5 hash of
  the sharding key (page number). As a result, pages are randomly distributed
  among the range-based shards.

  The keyspace_id is returned as a packed string. Use unpack_keyspace_id() to
  get the integer value if needed.

  For more about keyspace_id, see these references:
  - http://vitess.io/overview/concepts.html#keyspace-id
  - http://vitess.io/user-guide/sharding.html
  """
  m = hashlib.md5()
  m.update(uint64.pack(page))
  return m.digest()[:8]

def unpack_keyspace_id(keyspace_id):
  """Return the corresponding 64-bit unsigned integer for a keyspace_id."""
  return uint64.unpack(keyspace_id)[0]

@app.route("/")
def index():
  return app.send_static_file('index.html')

@app.route("/page/<int:page>")
def view(page):
  return app.send_static_file('index.html')

@app.route("/lrange/guestbook/<int:page>")
def list_guestbook(page):
  # Read the list from a replica.
  keyspace_id = get_keyspace_id(page)
  cursor = conn.cursor('test_keyspace', 'replica', keyspace_ids=[keyspace_id])

  cursor.execute(
      'SELECT message FROM messages WHERE page=%(page)s ORDER BY time_created_ns',
      {'page': page})
  entries = [row[0] for row in cursor.fetchall()]
  cursor.close()

  return json.dumps(entries)

@app.route("/rpush/guestbook/<int:page>/<value>")
def add_entry(page, value):
  # Insert a row on the master.
  keyspace_id = get_keyspace_id(page)
  keyspace_id_int = unpack_keyspace_id(keyspace_id)
  cursor = conn.cursor('test_keyspace', 'master', keyspace_ids=[keyspace_id], writable=True)

  cursor.begin()
  cursor.execute(
      'INSERT INTO messages (page, time_created_ns, keyspace_id, message)'
      ' VALUES (%(page)s, %(time_created_ns)s, %(keyspace_id)s, %(message)s)',
      {
        'page': page,
        'time_created_ns': int(time.time() * 1e9),
        'keyspace_id': keyspace_id_int,
        'message': value,
      })
  cursor.commit()

  # Read the list back from master (critical read) because it's
  # important that the user sees his own addition immediately.
  cursor.execute(
      'SELECT message FROM messages WHERE page=%(page)s ORDER BY time_created_ns',
      {'page': page})
  entries = [row[0] for row in cursor.fetchall()]
  cursor.close()

  return json.dumps(entries)

@app.route("/env")
def env():
  return json.dumps(dict(os.environ))

if __name__ == "__main__":
  timeout = 10 # connect timeout in seconds

  # Get vtgate service address from Kubernetes DNS.
  addr = 'vtgate:15001'

  # Connect to vtgate.
  conn = vtgatev2.connect({'vt': [addr]}, timeout)

  app.run(host='0.0.0.0', port=8080, debug=True)
