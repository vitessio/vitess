from vtdb import cursor

import framework

class TestStream(framework.TestCase):
  # UNION queries like this used to crash vtocc, only straight SELECT
  # would go through. This is a unit test to show it is fixed.
  # The fix went in revision bad7511746ca.
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

  def test_basic_stream(self):
    # insert 100 rows in a table
    self.env.conn.begin()
    for i in xrange(100):
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
        cu = self.env.execute("select * from vtocc_big b1, vtocc_big b2",
                              cursorclass=cursor.StreamCursor)
        count = 0
        while True:
          row = cu.fetchone()
          if row is None:
            break
          if count == 10:
            self.check_row_10(row)
          count += 1
        self.assertEqual(count, 10000)

      #row = cu.fetchone()
      #self.assertEqual(row, None)

  def check_row_10(self, row):
    # null the dates so they match
    row = list(row)
    row[6] = None
    row[11] = None
    row[20] = None
    row[25] = None
    self.assertEqual(row, [10L, 'AAAAAAAAAAAAAAAAAA 10', 'BBBBBBBBBBBBBBBBBB 10', 'C', 'DDDDDDDDDDDDDDDDDD 10', 'EEEEEEEEEEEEEEEEEE 10', None, 'FF 10', 'GGGGGGGGGGGGGGGGGG 10', 10L, 10L, None, 10L, 10, 0L, 'AAAAAAAAAAAAAAAAAA 0', 'BBBBBBBBBBBBBBBBBB 0', 'C', 'DDDDDDDDDDDDDDDDDD 0', 'EEEEEEEEEEEEEEEEEE 0', None, 'FF 0', 'GGGGGGGGGGGGGGGGGG 0', 0L, 0L, None, 0L, 0])
