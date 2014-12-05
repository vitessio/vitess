import json
import threading
import time
import traceback
import urllib

from vtdb import cursor
from vtdb import dbexceptions

import framework

class TestStream(framework.TestCase):
  def tearDown(self):
    self.env.conn.begin()
    self.env.execute("delete from vtocc_big")
    self.env.conn.commit()

  # UNION queries like this used to crash vtocc, only straight SELECT
  # would go through. This is a unit test to show it is fixed.
  def test_union(self):
    cu = self.env.execute("select 1 from dual union select 1 from dual",
                          cursorclass=cursor.StreamCursor)
    count = 0
    while True:
      row = cu.fetchone()
      if row is None:
        break
      count += 1
    self.assertEqual(count, 1)

  def test_customrules(self):
    bv = {'asdfg': 1}
    try:
      self.env.execute("select * from vtocc_test where intval=:asdfg", bv,
                       cursorclass=cursor.StreamCursor)
      self.fail("Bindvar asdfg should not be allowed by custom rule")
    except dbexceptions.DatabaseError as e:
      self.assertContains(str(e), "error: Query disallowed")

  def test_basic_stream(self):
    self._populate_vtocc_big_table(100)
    loop_count = 1

    # select lots of data using a non-streaming query
    if True:
      for i in xrange(loop_count):
        cu = self.env.execute("select * from vtocc_big b1, vtocc_big b2")
        rows = cu.fetchall()
        self.assertEqual(len(rows), 10000)
        self.check_row_10(rows[10])

    # select lots of data using a streaming query
    if True:
      for i in xrange(loop_count):
        cu = cursor.StreamCursor(self.env.conn)
        cu.execute("select * from vtocc_big b1, vtocc_big b2", {})
        count = 0
        while True:
          row = cu.fetchone()
          if row is None:
            break
          if count == 10:
            self.check_row_10(row)
          count += 1
        self.assertEqual(count, 10000)


  def test_streaming_error(self):
    with self.assertRaises(dbexceptions.DatabaseError):
      cu = self.env.execute("select count(abcd) from vtocc_big b1",
                            cursorclass=cursor.StreamCursor)

  def check_row_10(self, row):
    # null the dates so they match
    row = list(row)
    row[6] = None
    row[11] = None
    row[20] = None
    row[25] = None
    self.assertEqual(row, [10L, 'AAAAAAAAAAAAAAAAAA 10', 'BBBBBBBBBBBBBBBBBB 10', 'C', 'DDDDDDDDDDDDDDDDDD 10', 'EEEEEEEEEEEEEEEEEE 10', None, 'FF 10', 'GGGGGGGGGGGGGGGGGG 10', 10L, 10L, None, 10L, 10, 0L, 'AAAAAAAAAAAAAAAAAA 0', 'BBBBBBBBBBBBBBBBBB 0', 'C', 'DDDDDDDDDDDDDDDDDD 0', 'EEEEEEEEEEEEEEEEEE 0', None, 'FF 0', 'GGGGGGGGGGGGGGGGGG 0', 0L, 0L, None, 0L, 0])
  
  def test_streaming_terminate(self):
    try:
      self._populate_vtocc_big_table(100)
      query = 'select * from vtocc_big b1, vtocc_big b2, vtocc_big b3'
      cu = cursor.StreamCursor(self.env.conn)
      thd = threading.Thread(target=self._stream_exec, args=(cu,query))
      thd.start()
      tablet_addr = "http://" + self.env.conn.addr
      connId = self._get_conn_id(tablet_addr)
      self._terminate_query(tablet_addr, connId)
      thd.join()
      with self.assertRaises(dbexceptions.DatabaseError) as cm:
        cu.fetchall()
      errMsg1 = "error: Lost connection to MySQL server during query (errno 2013)"
      errMsg2 = "error: Query execution was interrupted (errno 1317)"
      self.assertTrue(cm.exception not in (errMsg1, errMsg2), "did not raise interruption error: %s" % str(cm.exception))
      cu.close()
    except Exception, e:
      self.fail("Failed with error %s %s" % (str(e), traceback.print_exc()))

  def _populate_vtocc_big_table(self, num_rows):
      self.env.conn.begin()
      for i in xrange(num_rows):
        self.env.execute("insert into vtocc_big values " +
                       "(" + str(i) + ", " +
                       "'AAAAAAAAAAAAAAAAAA " + str(i) + "', " +
                       "'BBBBBBBBBBBBBBBBBB " + str(i) + "', " +
                       "'C', " +
                       "'DDDDDDDDDDDDDDDDDD " + str(i) + "', " +
                       "'EEEEEEEEEEEEEEEEEE " + str(i) + "', " +
                       "now()," +
                       "'FF " + str(i) + "', " +
                       "'GGGGGGGGGGGGGGGGGG " + str(i) + "', " +
                       str(i) + ", " +
                       str(i) + ", " +
                       "now()," +
                       str(i) + ", " +
                       str(i%100) + ")")
      self.env.conn.commit()

  # Initiate a slow stream query
  def _stream_exec(self, cu, query):
      cu.execute(query, {})

  # Get the connection id from status page 
  def _get_conn_id(self, tablet_addr):
      streamqueryz_url = tablet_addr +  "/streamqueryz?format=json" 
      retries = 3
      streaming_queries = []
      while len(streaming_queries) == 0:
        content = urllib.urlopen(streamqueryz_url).read()
        streaming_queries = json.loads(content)
        retries -= 1
        if retries == 0:
            self.fail("unable to fetch streaming queries from %s" % streamqueryz_url)
        else:
            time.sleep(1) 
      connId = streaming_queries[0]['ConnID']
      return connId

  # Terminate the query via streamqueryz admin page
  def _terminate_query(self, tablet_addr, connId):
      terminate_url = tablet_addr + "/streamqueryz/terminate?format=json&connID=" + str(connId)
      urllib.urlopen(terminate_url).read()
