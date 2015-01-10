import os
import json

from flask import Flask
app = Flask(__name__)

from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import vtgatev2
from vtdb import vtgate_cursor
from vtdb import topology
from zk import zkocc

# Constants and params
UNSHARDED = [keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)]

# conn is the connection to vtgate.
conn = None

@app.route("/")
def index():
  return app.send_static_file('index.html')

@app.route("/lrange/guestbook")
def list_guestbook():
  # Read the list from a replica.
  cursor = conn.cursor('test_keyspace', 'replica', keyranges=UNSHARDED)

  cursor.execute('SELECT * FROM test_table ORDER BY id', {})
  entries = [row[1] for row in cursor.fetchall()]
  cursor.close()

  return json.dumps(entries)

@app.route("/rpush/guestbook/<value>")
def add_entry(value):
  # Insert a row on the master.
  cursor = conn.cursor('test_keyspace', 'master', keyranges=UNSHARDED, writable=True)

  cursor.begin()
  cursor.execute('INSERT INTO test_table (msg) VALUES (%(msg)s)',
    {'msg': value})
  cursor.commit()

  # Read the list back from master (critical read) because it's
  # important that the user sees his own addition immediately.
  cursor.execute('SELECT * FROM test_table ORDER BY id', {})
  entries = [row[1] for row in cursor.fetchall()]
  cursor.close()

  return json.dumps(entries)

@app.route("/env")
def env():
  return json.dumps(dict(os.environ))

if __name__ == "__main__":
  timeout = 10 # connect timeout in seconds

  # Get vtgate service address from Kubernetes environment.
  addr = '%s:%s' % (os.environ['VTGATE_SERVICE_HOST'], os.environ['VTGATE_SERVICE_PORT'])

  # Read topology from vtgate.
  # This is a temporary workaround until the VTGate V2 client is topology-free.
  topoconn = zkocc.ZkOccConnection(addr, 'test', timeout)
  topology.read_topology(topoconn)
  topoconn.close()

  # Connect to vtgate.
  conn = vtgatev2.connect({'_vt': [addr]}, timeout)

  app.run(host='0.0.0.0', port=8080, debug=True)
